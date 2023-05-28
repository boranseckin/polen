#![allow(clippy::needless_return)]

use std::io::{StdoutLock, Write};

use anyhow::{Context, bail};
use serde::{Serialize, Deserialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
struct Message {
    src: String,
    dest: String,
    body: Body,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct Body {
    #[serde(rename = "msg_id")]
    id: Option<usize>,
    in_reply_to: Option<usize>,
    #[serde(flatten)]
    payload: Payload,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
enum Payload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk,

    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
}

struct Node {
    id: usize,
}

impl Node {
    fn step(&mut self, input: Message, output: &mut StdoutLock) -> anyhow::Result<()> {
        match input.body.payload {
            Payload::Init { .. } => {
                let reply = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::InitOk,
                    }
                };

                serde_json::to_writer(&mut *output, &reply)?;
                output.write_all(b"\n")?;

                self.id += 1;
            },
            Payload::InitOk => bail!("node received init_ok message"),

            Payload::Echo { echo } => {
                let reply = Message {
                    src: input.dest,
                    dest: input.src,
                    body: Body {
                        id: Some(self.id),
                        in_reply_to: input.body.id,
                        payload: Payload::EchoOk {
                            echo,
                        },
                    }
                };

                serde_json::to_writer(&mut *output, &reply)?;
                output.write_all(b"\n")?;

                self.id += 1;
            },
            Payload::EchoOk { .. } => {},
        };

        return Ok(());
    }
}

fn main() -> anyhow::Result<()> {
    let stdin = std::io::stdin().lock();
    let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();

    let mut stdout = std::io::stdout().lock();

    let mut node = Node { id: 0 };

    for input in inputs {
        let input = input.context("input from stdin could not be deserialized")?;
        node.step(input, &mut stdout)?;
    }

    return Ok(());
}
