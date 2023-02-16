use anyhow::{anyhow, Result};
use clap::Parser;
use glob::glob;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::iter::{IntoIterator, Peekable};
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

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Question,
    Star,
    Or,
    LeftParen,
    RightParen,
    Dot,
    Plus,
    AlphaNum(String),
}

fn tokenize(input: &str) -> Result<Vec<Token>> {
    let mut tokens = vec![];
    let mut alphanum = String::new();
    for ch in input.chars() {
        let symbol = match ch {
            '|' => Some(Token::Or),
            '?' => Some(Token::Question),
            '(' => Some(Token::LeftParen),
            ')' => Some(Token::RightParen),
            '*' => Some(Token::Star),
            '.' => Some(Token::Dot),
            '+' => Some(Token::Plus),
            _ => None,
        };
        if let Some(symbol) = symbol {
            if !alphanum.is_empty() {
                tokens.push(Token::AlphaNum(alphanum));
                alphanum = String::new();
            }
            tokens.push(symbol);
        } else {
            alphanum.push(ch);
        }
    }
    if !alphanum.is_empty() {
        tokens.push(Token::AlphaNum(alphanum));
    }
    return Ok(tokens);
}

// EXPR = EXPR | EXPR
// EXPR = text
#[derive(PartialEq, Eq, Debug)]
enum Expr {
    ZeroOrOne(Box<Expr>),
    ZeroOrMore(Box<Expr>),
    OneOrMore(Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Text(String),
    Dot,
    Group(Box<Expr>),
}

struct TokenStream {
    iter: Peekable<std::vec::IntoIter<Token>>,
}

impl TokenStream {
    fn new(tokens: Vec<Token>) -> Self {
        Self {
            iter: tokens.into_iter().peekable(),
        }
    }

    fn peek(&mut self) -> Result<&Token> {
        self.iter.peek().ok_or_else(|| anyhow!("EOF"))
    }

    fn expect(&mut self, t: Token) -> Result<()> {
        self.next().and_then(|a| {
            if a == t {
                Ok(())
            } else {
                Err(anyhow!("expected {:?}, got {:?}", t, a))
            }
        })
    }

    fn next(&mut self) -> Result<Token> {
        self.iter.next().ok_or_else(|| anyhow!("EOF"))
    }

    fn has_more(&mut self) -> bool {
        self.iter.peek().is_some()
    }
}

// REGEX:
//   TERM '|' TERM
// TERM:
//   FACTOR*
// FACTOR:
//    BASE '?'
//    BASE '*'
//    BASE '+'
// BASE:
//   char | '(' REGEX ')' | .
//
// char: A-Za-z0-9

fn regex(stream: &mut TokenStream) -> Result<Expr> {
    let left = term(stream)?;
    Ok(if stream.has_more() && stream.peek()? == &Token::Or {
        stream.expect(Token::Or)?;
        let right = regex(stream)?;
        Expr::Or(Box::new(left), Box::new(right))
    } else {
        left
    })
}

fn term(stream: &mut TokenStream) -> Result<Expr> {
    let mut expr = factor(stream)?;
    while stream.has_more() && stream.peek()? != &Token::RightParen && stream.peek()? != &Token::Or
    {
        let next = factor(stream)?;
        expr = Expr::And(Box::new(expr), Box::new(next));
    }
    Ok(expr)
}

//         [Token::AlphaNum(a), Token::Question, tail @ ..] => Expr::And(
//             Box::new(Expr::Text(a.as_str()[0..a.len() - 1].to_string())),
//             Box::new(Expr::And(
//                 Box::new(Expr::ZeroOrOne(Box::new(Expr::Text(
//                     a.as_str()[a.len() - 1..a.len()].to_string(),
//                 )))),
//                 Box::new(parse(tail)?),
//             )),
//         ),
fn factor(stream: &mut TokenStream) -> Result<Expr> {
    let expr = base(stream)?;
    if !stream.has_more() {
        return Ok(expr);
    }

    let expr_factory = match stream.peek()? {
        Token::Question => Expr::ZeroOrOne,
        Token::Star => Expr::ZeroOrMore,
        Token::Plus => Expr::OneOrMore,
        _ => return Ok(expr),
    };

    stream.next()?;

    Ok(if let Expr::Text(text) = &expr {
        let (prefix, suffix) = text.split_at(text.len() - 1);
        Expr::And(
            Box::new(Expr::Text(prefix.to_string())),
            Box::new(expr_factory(Box::new(Expr::Text(suffix.to_string())))),
        )
    } else {
        expr_factory(Box::new(expr))
    })
}

fn base(stream: &mut TokenStream) -> Result<Expr> {
    if stream.peek()? == &Token::LeftParen {
        stream.expect(Token::LeftParen)?;
        let inner = regex(stream)?;
        stream.expect(Token::RightParen)?;
        Ok(Expr::Group(Box::new(inner)))
    } else {
        match stream.next()? {
            Token::AlphaNum(text) => Ok(Expr::Text(text)),
            Token::Dot => Ok(Expr::Dot),
            t => Err(anyhow!("expected alpha num, got {:?}", t)),
        }
    }
}

fn parse(tokens: &mut TokenStream) -> Result<Expr> {
    regex(tokens)
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
        let expr = parse(&mut TokenStream::new(tokens))?;
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
            Expr::OneOrMore(expr) | Expr::Group(expr) => self.eval(*expr),
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
                doc_set.unwrap_or_else(|| (0..self.doc_sources.len()).collect())
            }
            Expr::Or(left, right) => &self.eval(*left) | &self.eval(*right),
            Expr::And(left, right) => &self.eval(*left) & &self.eval(*right),
            Expr::ZeroOrMore(_) | Expr::ZeroOrOne(_) | Expr::Dot => {
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
    let end = input
        .grapheme_indices(true)
        .chain(std::iter::once((input.len(), &input[0..0])))
        .skip(3);
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
            index.search("Google?").expect("search failed"),
            vec![s!("test_data/file1"), s!("test_data/file2")],
        );
        assert_eq_unordered!(
            index.search("Google ?Search").expect("search failed"),
            vec![s!("test_data/file1")],
        );
        assert_eq_unordered!(
            index.search("Google z?Search").expect("search failed"),
            vec![s!("test_data/file1")],
        );
    }

    #[test]
    fn tokenize_simple() {
        assert_eq!(tokenize("abcd").unwrap(), vec![Token::AlphaNum(s!("abcd"))]);
        assert_eq!(
            tokenize("ab|cd").unwrap(),
            vec![
                Token::AlphaNum(s!("ab")),
                Token::Or,
                Token::AlphaNum(s!("cd"))
            ]
        );
        assert_eq!(
            tokenize("ab|").unwrap(),
            vec![Token::AlphaNum(s!("ab")), Token::Or]
        );
        assert_eq!(
            tokenize("|ab").unwrap(),
            vec![Token::Or, Token::AlphaNum(s!("ab"))]
        );
        assert_eq!(tokenize("|").unwrap(), vec![Token::Or]);
        assert_eq!(tokenize("||").unwrap(), vec![Token::Or, Token::Or]);
    }

    fn parse_stuff(input: &str) -> Expr {
        let tokens = tokenize(input).expect("failed to tokenize");
        parse(&mut TokenStream::new(tokens)).expect("failed to parse tokens")
    }

    #[test]
    fn parse_simple() {
        assert_eq!(parse_stuff("abcd"), Expr::Text(s!("abcd")));
        assert_eq!(
            parse_stuff("abc|def|ghi"),
            Expr::Or(
                Box::new(Expr::Text(s!("abc"))),
                Box::new(Expr::Or(
                    Box::new(Expr::Text(s!("def"))),
                    Box::new(Expr::Text(s!("ghi"))),
                ))
            )
        );
        assert_eq!(
            parse_stuff("abc?d"),
            Expr::And(
                Box::new(Expr::And(
                    Box::new(Expr::Text(s!("ab"))),
                    Box::new(Expr::ZeroOrOne(Box::new(Expr::Text(s!("c")))))
                )),
                Box::new(Expr::Text(s!("d")))
            )
        );
    }

    #[test]
    fn search_parentheses() {
        let docs = make_docs(["Google", "Googly"]);
        let index = Index::new_with_sources(docs);

        assert_eq!(
            index.search("Goo(abcd)gle").expect("search failed"),
            Vec::<String>::new()
        );

        assert_eq!(
            index.search("Goo(abcd)?gle").expect("search failed"),
            vec![s!("Google")],
        );
    }

    #[test]
    fn search_one_or_more() {
        let docs = make_docs(["Google", "Gooooogle", "Fun"]);
        let index = Index::new_with_sources(docs);

        assert_eq!(
            index.search("Goo(ooo)+gle").expect("search failed"),
            vec![s!("Gooooogle")]
        );

        assert_eq_unordered!(
            index.search("G(o)+gle").expect("search failed"), // try to match docs with: ANY AND gle
            vec![s!("Google"), s!("Gooooogle")],
        );

        assert_eq_unordered!(
            index.search("Goo+gle").expect("search failed"),
            vec![s!("Google"), s!("Gooooogle")],
        );
    }

    #[test]
    fn search_dot() {
        let docs = make_docs(["Google", "Gooooogle", "Fun"]);
        let index = Index::new_with_sources(docs);

        assert_eq_unordered!(
            index.search("Goo.gle").expect("search failed"),
            vec![s!("Google"), s!("Gooooogle")]
        );

        assert_eq_unordered!(
            index.search("Go.+gle").expect("search failed"),
            vec![s!("Google"), s!("Gooooogle")],
        );
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
