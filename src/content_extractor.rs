use regex::Regex;
use std::ops::Index;
use std::iter::FromIterator;

#[derive(Clone, PartialEq)]
enum TagType {
    Start,
    End
}

#[derive(Clone)]
struct Tag {
    tag_type: TagType,
    tag_name: String,
    start_index: usize,
    end_index: usize,
}

pub struct Content {
    pub title: Option<String>,
    pub body: String
}

impl Content {
    pub fn new(title: Option<String>, body: String) -> Content {
        Content {
            title,
            body
        }
    }
}

pub struct ContentExtractor {

}

impl ContentExtractor {
    pub fn new() -> ContentExtractor {
        ContentExtractor {

        }
    }

    fn extract_tags(&self, content: &str) -> Vec<Tag> {
        let mut tags: Vec<Tag> = Vec::new();

        let regex = Regex::new("</?([^>]+)>").unwrap();
        for tag_match in regex.captures_iter(content) {
            let whole_tag = tag_match.get(0).unwrap();
            let tag = whole_tag.as_str();
            let tag_inner = tag_match.get(1).unwrap().as_str();
            let tag_name = tag_inner.split(" ").next().unwrap();

            if tag.starts_with("</") {
                if tags.len() > 0 && tags.last().unwrap().tag_type == TagType::End {
                    tags.push(Tag {
                        tag_type: TagType::Start,
                        tag_name: "".to_owned(),
                        start_index: tags.last().unwrap().start_index,
                        end_index: tags.last().unwrap().end_index,
                    });

                    tags.push(Tag {
                        tag_type: TagType::End,
                        tag_name: "".to_owned(),
                        start_index: whole_tag.start(),
                        end_index: whole_tag.end()
                    });
                }

                tags.push(Tag {
                    tag_type: TagType::End,
                    tag_name: tag_name.to_owned(),
                    start_index: whole_tag.start(),
                    end_index: whole_tag.end(),
                });
            } else {
                if tags.len() > 0 {
                    tags.push(Tag {
                        tag_type: TagType::Start,
                        tag_name: "".to_owned(),
                        start_index: tags.last().unwrap().start_index,
                        end_index: tags.last().unwrap().end_index,
                    });

                    tags.push(Tag {
                        tag_type: TagType::End,
                        tag_name: "".to_owned(),
                        start_index: whole_tag.start(),
                        end_index: whole_tag.end(),
                    });
                }

                tags.push(Tag {
                    tag_type: TagType::Start,
                    tag_name: tag_name.to_owned(),
                    start_index: whole_tag.start(),
                    end_index: whole_tag.end(),
                });
            }
        }

        tags
    }

    pub fn extract(&self, content: &str) -> Content {
        let tags = self.extract_tags(content);

        let mut title: Option<String> = None;
        let mut extracted = String::new();

        let content_array = content.bytes().collect::<Vec<_>>();

        struct TagStackEntry {
            index: usize,
            tag: Tag
        }

        let mut tag_stack = Vec::<TagStackEntry>::new();
        let mut ignore_tag: Option<String> = None;

        for (index, tag) in tags.iter().enumerate() {
            match tag.tag_type {
                TagType::Start => {
                    tag_stack.push(TagStackEntry {
                       index,
                       tag: tag.clone()
                    });

                    if tag.tag_name == "sup" {
                        ignore_tag = Some(tag.tag_name.to_owned());
                    }
                },
                TagType::End => {
                    let start_tag = tag_stack.pop().unwrap();
                    if ignore_tag.is_some() {
                        if ignore_tag.as_ref().unwrap() == &tag.tag_name {
                            ignore_tag = None;
                        }

                        continue;
                    }

                    if index - start_tag.index == 1 && start_tag.tag.tag_name == tag.tag_name {
                        if start_tag.tag.tag_name == "script" || start_tag.tag.tag_name == "style" {
                            continue;
                        }

                        let extracted_tag_content = std::str::from_utf8(&content_array[start_tag.tag.end_index..tag.start_index]).unwrap();

                        if start_tag.tag.tag_name != "title" {
                            extracted += extracted_tag_content;
                        } else {
                            title = Some(extracted_tag_content.to_owned());
                        }
                    }
                }
            }
        }

        Content::new(title, extracted)
    }
}

#[test]
fn test_extract1() {
    let content = "<div>Testing.</div>";
    let extractor = ContentExtractor::new();
    let extracted = extractor.extract(content);
    assert_eq!("Testing.", &extracted.body)
}

#[test]
fn test_extract2() {
    let content = "<div><p>Testing.</p></div>";
    let extractor = ContentExtractor::new();
    let extracted = extractor.extract(content);
    assert_eq!("Testing.", &extracted.body)
}

#[test]
fn test_extract3() {
    let content = "<div>Hello <b>my</b> friend.</div>";
    let extractor = ContentExtractor::new();
    let extracted = extractor.extract(content);
    assert_eq!("Hello my friend.", &extracted.body)
}

#[test]
fn test_extract4() {
    let content = "<div><span>Hello </span><b>my</b><span> friend.</span></div>";
    let extractor = ContentExtractor::new();
    let extracted = extractor.extract(content);
    assert_eq!("Hello my friend.", &extracted.body)
}

#[test]
fn test_extract5() {
    let content = "<div><b>Haha</b><i>Wololo</i></div>";
    let extractor = ContentExtractor::new();
    let extracted = extractor.extract(content);
    assert_eq!("HahaWololo", &extracted.body)
}

#[test]
fn test_extract6() {
    let content = "<div><b>Haha</b> <i>Wololo</i></div>";
    let extractor = ContentExtractor::new();
    let extracted = extractor.extract(content);
    assert_eq!("Haha Wololo", &extracted.body)
}

#[test]
fn test_extract7() {
    let content = "<div><b>Haha</b> <i>Wololo</k></div>";
    let extractor = ContentExtractor::new();
    let extracted = extractor.extract(content);
    assert_eq!("Haha ", &extracted.body)
}

#[test]
fn test_extract8() {
    let content = "<div><b>Haha</b> <i>Wololo</k>Again</div>";
    let extractor = ContentExtractor::new();
    let extracted = extractor.extract(content);
    assert_eq!("Haha Again", &extracted.body)
}

#[test]
fn test_extract9() {
    let content = "<div><b>Haha</b> <a href=\"wololo\">Wololo</a> Again</div>";
    let extractor = ContentExtractor::new();
    let extracted = extractor.extract(content);
    assert_eq!("Haha Wololo Again", &extracted.body)
}

#[test]
fn test_extract10() {
    let content = "<div><b>Haha</b> <script>Ignore this</script>Again</div>";
    let extractor = ContentExtractor::new();
    let extracted = extractor.extract(content);
    assert_eq!("Haha Again", &extracted.body)
}

#[test]
fn test_extract11() {
    let content = "<div><b>Haha</b> <sup><b>Ignore this</b></sup>Again</div>";
    let extractor = ContentExtractor::new();
    let extracted = extractor.extract(content);
    assert_eq!("Haha Again", &extracted.body)
}

#[test]
fn test_extract12() {
    let content = "<html><title>Hello, World!</title><div><b>Haha</b> <sup><b>Ignore this</b></sup>Again</div></html>";
    let extractor = ContentExtractor::new();
    let extracted = extractor.extract(content);
    assert_eq!("Hello, World!", &extracted.title.unwrap());
    assert_eq!("Haha Again", &extracted.body)
}

#[test]
fn test_extract_full_page() {
    let content = std::fs::read_to_string("testdata/test_full_page.txt").unwrap();
    let extractor = ContentExtractor::new();
    extractor.extract(&content);
}