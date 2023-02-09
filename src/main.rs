use anyhow::{anyhow, Result};
use clap::Parser;
use glob::glob;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::iter::IntoIterator;
use std::path::PathBuf;
use unicode_segmentation::UnicodeSegmentation;

// https://swtch.com/~rsc/regexp/regexp4.html

// Ex:
//
// dataset:
//
// 1 Google Code Search
// 2 Google Code Project Hosting
// 3 Google Web Search
// 4 Go
//
// dataset.search("Code") => ["Google Code Search", "Google Code Project Hosting"]
// dataset.search("Google") => ["Google Code Search", "Google Code Project Hosting", "Google Web Search"]
// dataset.search("") => []
//
type DocIndex = usize; // index into list of documents

// "Goo" => [1,2,3,4]
// "Go" => [4]

pub struct Index {
    trigram_index: HashMap<String, HashSet<DocIndex>>,
    doc_sources: Vec<DocSource>,
}

#[derive(Debug)]
pub enum DocSource {
    Disk(PathBuf),
    Mem(String),
}

fn tokenize(input: &str) -> Result<Vec<String>> {
    let mut tokens = vec![];
    let mut token = String::new();
    for ch in input.chars() {
        if ch == '|' || ch == '?' {
            if !token.is_empty() {
                tokens.push(token);
                token = String::new();
            }
            tokens.push(ch.to_string());
        } else {
            token.push(ch);
        }
    }
    if !token.is_empty() {
        tokens.push(token);
    }
    return Ok(tokens)
}

// EXPR = EXPR | EXPR
// EXPR = text
#[derive(PartialEq, Eq, Debug)]
enum Expr {
    ZeroOrOne(Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Text(String),
    Nil,
}

fn parse(tokens: &[String]) -> Result<Expr> {
    // check if its an or expression:
    Ok(match tokens {
        // abc?def
        // a="abc" op="?" tail="def"
        // => "ab" AND "c?" AND "def"
        [a, op, tail @ ..] if op == "?" => Expr::And(
            Box::new(Expr::Text(a.as_str()[0..a.len() - 1].to_string())),
            Box::new(Expr::And(
                Box::new(Expr::ZeroOrOne(Box::new(Expr::Text(a.as_str()[a.len() - 1..a.len()].to_string())))),
                Box::new(parse(tail)?),
            )),
        ),
        [a, op, tail @ ..] if op == "|" => Expr::Or(Box::new(Expr::Text(a.clone())), Box::new(parse(tail)?)),
        [a] => Expr::Text(a.clone()),
        [] => Expr::Nil,
        rest => return Err(anyhow!("unsupported syntax: {:?}", rest)),
    })
}

impl Index {
    /// Creates a new empty [`Index`].
    /// Populate the index with [`Index::update_index`].
    pub fn new() -> Self {
        Self {
            trigram_index: HashMap::new(),
            doc_sources: Vec::new(),
        }
    }

    /// Creates a new [`Index`] over an initial set of documents. Additional documents
    /// can be added later with [`Index::update_index`].
    /// Any errors with reading the contents of a [`DocSource::Disk`] file are ignored.
    pub fn new_with_sources(sources: impl IntoIterator<Item = DocSource>) -> Self {
        let mut index = Index::new();
        for source in sources {
            index.update_index(source);
        }
        index
    }

    /// Updates the index by adding a new source. Duplicate source documents are not
    /// removed.
    /// Any errors with reading the contents of a [`DocSource::Disk`] file are ignored.
    pub fn update_index(&mut self, doc: DocSource) {
        match &doc {
            DocSource::Mem(contents) => {
                self.update_index_impl(self.doc_sources.len(), contents);
            }
            DocSource::Disk(path) => {
                let contents = match fs::read_to_string(path) {
                    Ok(contents) => contents,
                    Err(_) => {
                        // Skip unreadable files, cuz fuck 'em.
                        return;
                    }
                };
                self.update_index_impl(self.doc_sources.len(), contents.as_str());
            }
        }
        self.doc_sources.push(doc);
    }

    fn update_index_impl(&mut self, idx: DocIndex, doc_contents: &str) {
        // TODO: what if document is < length 3?
        for trigram in split_into_trigrams(doc_contents) {
            let doc_list = self
                .trigram_index
                .entry(trigram.to_string())
                .or_insert_with(|| HashSet::new());
            doc_list.insert(idx);
        }
    }

    /// Search the indexed documents for `term`.
    /// The search will only return exact matches. The `term` must not be less than 3 characters
    /// in length.
    pub fn search(&self, term: &str) -> Result<Vec<String>> {
        if term.len() < 3 {
            return Err(anyhow!("too short"));
        }

        let tokens = tokenize(term)?;
        let expr = parse(&tokens)?;
        eprintln!("Search expr: {:#?}", &expr);

        // AND all trigrams means that each trigram must be present in the index.
        let doc_set = self.eval(expr);

        Ok(doc_set
            .into_iter()
            .map(|idx| match &self.doc_sources[idx] {
                DocSource::Mem(contents) => contents.clone(),
                DocSource::Disk(path) => path.display().to_string(),
            })
            .collect())
    }

    fn eval(&self, expr: Expr) -> HashSet<DocIndex> {
        match expr {
            Expr::Text(term) => {
                let mut doc_set: Option<HashSet<DocIndex>> = None;
                for trigram in split_into_trigrams(&term) {
                    if let Some(doc_candidates) = self.trigram_index.get(trigram) {
                        if let Some(doc_set) = doc_set.as_mut() {
                            *doc_set = &*doc_set & doc_candidates;
                        } else {
                            doc_set = Some(doc_candidates.clone());
                        }
                    } else {
                        return HashSet::new();
                    }
                }
                doc_set.unwrap_or_default()
            }
            Expr::Or(left, right) => {
                &self.eval(*left) | &self.eval(*right)
            }
            Expr::And(left, right) => {
                &self.eval(*left) & &self.eval(*right)
            }
            Expr::Nil | Expr::ZeroOrOne(_) => {
                (0..self.doc_sources.len()).collect()
            }
        }
    }
}

/// Returns all trigrams from the input.
///
/// # Examples
/// "Google" => ["Goo", "oog", "ogl", "gle"]
/// "Hi There" => ["Hi ", "i T", ...]
/// "Go" => ["Go"]
fn split_into_trigrams<'a>(input: &'a str) -> impl Iterator<Item = &'a str> {
    let start = input.grapheme_indices(true);
    let end = input.grapheme_indices(true).chain(std::iter::once((input.len(), &input[0..0]))).skip(3);
    let iter = start.zip(end);
    iter.map(|((start, _), (end, _))| &input[start..end])
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Directory to search, defaults to current directory.
    #[arg(short, long)]
    dir: Option<String>,

    /// Turn on verbose logging.
    #[arg(short, long)]
    verbose: bool,

    /// The pattern to search.
    pattern: String,
}

fn main() {
    let args = Args::parse();
    let dir = PathBuf::from(args.dir.unwrap_or_else(|| String::from("./**/*")));
    let dir_str = dir.to_str().expect("cannot fail");
    let paths = glob(dir_str)
        .unwrap()
        .filter_map(|entry| match entry {
            Ok(path) if path.is_file() => Some(Ok(DocSource::Disk(path))),
            Ok(_) => None,
            Err(err) => Some(Err(err)),
        })
        .collect::<Result<Vec<DocSource>, _>>()
        .expect("failed to glob files from dir");
    if args.verbose {
        eprintln!("Paths being indexed: {:?}", &paths);
    }

    let index = Index::new_with_sources(paths);

    for (idx, result) in index
        .search(&args.pattern)
        .expect("failed search")
        .iter()
        .enumerate()
    {
        println!("({}) {}", idx, result);
    }

    if args.verbose {
        println!("# of trigrams: {}", index.trigram_index.len());
        println!("# of documents indexed: {}", index.doc_sources.len());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_unordered::assert_eq_unordered;
    use maplit::{hashmap, hashset};

    macro_rules! s {
        ($l:literal) => {
            String::from($l)
        };
    }

    fn make_docs<const N: usize>(docs: [&str; N]) -> Vec<DocSource> {
        docs.into_iter()
            .map(String::from)
            .map(DocSource::Mem)
            .collect()
    }

    #[test]
    fn test_make_index_from_strings_single() {
        let docs = make_docs(["Google"]);
        let index = Index::new_with_sources(docs);
        assert_eq!(
            index.trigram_index,
            hashmap! {
                s!("Goo") => hashset! { 0 },
                s!("oog") => hashset! { 0 },
                s!("ogl") => hashset! { 0 },
                s!("gle") => hashset! { 0 },
            }
        );
    }

    #[test]
    fn test_make_index_from_strings_multi() {
        let docs = make_docs(["Google", "Googly", "oooooo"]);
        let index = Index::new_with_sources(docs);
        assert_eq!(
            index.trigram_index,
            hashmap! {
                s!("Goo") => hashset! { 0, 1 },
                s!("oog") => hashset! { 0, 1 },
                s!("ogl") => hashset! { 0, 1 },
                s!("gle") => hashset! { 0 },
                s!("gly") => hashset! { 1 },
                s!("ooo") => hashset! { 2 },
            }
        );
    }

    #[test]
    fn test_search_strings_simple() {
        let docs = make_docs(["Google", "Googly"]);
        let index = Index::new_with_sources(docs);

        assert_eq!(
            index.search("Google").expect("search failed"),
            vec![s!("Google")],
        );
    }

    #[test]
    fn test_search_strings_or() {
        let docs = make_docs(["The first bard", "The second clown"]);
        let index = Index::new_with_sources(docs);

        assert_eq_unordered!(
            index.search("first|second").expect("search failed"),
            vec![s!("The first bard"), s!("The second clown")],
        );

        assert_eq_unordered!(
            index.search("The ").expect("search failed"),
            vec![s!("The first bard"), s!("The second clown")],
        );

        assert_eq!(
            index.search("first").expect("search failed"),
            vec![s!("The first bard")],
        );
    }

    // https://docs.rs/glob/latest/glob/
    #[test]
    fn test_search_simple_terms() {
        let index = Index::new_with_sources(
            glob("test_data/*")
                .unwrap()
                .map(|entry| DocSource::Disk(entry.unwrap())),
        );

        assert_eq_unordered!(
            index.search("Google").expect("search failed"),
            vec![s!("test_data/file1")],
        );
        assert_eq_unordered!(
            index.search("Googly").expect("search failed"),
            vec![s!("test_data/file2")],
        );
        assert_eq_unordered!(
            index.search("Goo").expect("search failed"),
            vec![s!("test_data/file1"), s!("test_data/file2")],
        );
        assert_eq_unordered!(
            index
                .search("search term not found")
                .expect("search failed"),
            Vec::<String>::new(),
        );
    }

    #[test]
    fn test_search_regex() {
        let index = Index::new_with_sources(
            glob("test_data/*")
                .unwrap()
                .map(|entry| DocSource::Disk(entry.unwrap())),
        );

        assert_eq_unordered!(
            index
                .search("Google?")
                .expect("search failed"),
            vec![s!("test_data/file1"), s!("test_data/file2")],
        );
        assert_eq_unordered!(
            index
                .search("Google ?Search")
                .expect("search failed"),
            vec![s!("test_data/file1")],
        );
        assert_eq_unordered!(
            index
                .search("Google z?Search")
                .expect("search failed"),
            vec![s!("test_data/file1")],
        );
    }

    #[test]
    fn tokenize_simple() {
        assert_eq!(tokenize("abcd").unwrap(), vec![s!("abcd")]);
        assert_eq!(tokenize("ab|cd").unwrap(), vec![s!("ab"), s!("|"), s!("cd")]);
        assert_eq!(tokenize("ab|").unwrap(), vec![s!("ab"), s!("|")]);
        assert_eq!(tokenize("|ab").unwrap(), vec![s!("|"), s!("ab")]);
        assert_eq!(tokenize("|").unwrap(), vec![s!("|")]);
        assert_eq!(tokenize("||").unwrap(), vec![s!("|"), s!("|")]);
    }

    fn parse_stuff(input: &str) -> Expr {
        let tokens = tokenize(input).expect("failed to tokenize");
        parse(&tokens).expect("failed to parse tokens")
    }

    #[test]
    fn parse_simple() {
        assert_eq!(parse_stuff("abcd"), Expr::Text(s!("abcd")));
        assert_eq!(parse_stuff("abc|def|ghi"), Expr::Or(
            Box::new(Expr::Text(s!("abc"))),
            Box::new(Expr::Or(
                Box::new(Expr::Text(s!("def"))),
                Box::new(Expr::Text(s!("ghi"))),
            ))
        ));
        assert_eq!(parse_stuff("abc?d"), Expr::And(
            Box::new(Expr::Text(s!("ab"))),
            Box::new(Expr::And(
                Box::new(Expr::ZeroOrOne(Box::new(Expr::Text(s!("c"))))),
                Box::new(Expr::Text(s!("d"))),
            ))
        ));
    }
}

//
// EXPR = EXPR | EXPR
// EXPR = text
//

// o+ = suffix(o) = o

// [Go][o*][gle] = ANY AND match(o*) AND gle
// match(Go) AND match(o*) AND match(gle)
// any AND any 

// match(Go) AND match(o*) AND gle
//
//  match(e1e2)	=	match(e1) AND match(e2)
// match(Go) = ANY
// match(Goo) = Goo
// match(Goog) = Goo AND oog
// Go = match(G) AND match(o)
// match(G) = ANY
// Go = match(ANY) AND match(ANY)
// Go = match(ANY)l