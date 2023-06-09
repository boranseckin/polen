#![allow(clippy::needless_return)]

use std::{io::{StdoutLock, Write}, collections::HashMap};

use anyhow::{Context, bail};
use serde::{Serialize, Deserialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
struct Message {
    src: String,
    dest: String,
    body: Body,
}

impl Message {
    fn reply(self, node: &mut Node, payload: Payload, output: &mut StdoutLock) -> anyhow::Result<()> {
        let reply = Self {
            src: node.node_id.clone().expect("node to be initialized"),
            dest: self.src,
            body: Body {
                msg_id: Some(node.msg_id),
                in_reply_to: self.body.msg_id,
                payload,
            },
        };

        serde_json::to_writer(&mut *output, &reply)?;
        output.write_all(b"\n")?;

        node.msg_id += 1;

        return Ok(());
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct Body {
    msg_id: Option<usize>,
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

    Generate,
    GenerateOk {
        id: String,
    },

    Broadcast {
        message: usize,
    },
    BroadcastOk,

    Read,
    ReadOk {
        messages: Vec<usize>,
    },

    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
}

struct Node {
    node_id: Option<String>,
    msg_id: usize,
    messages: Vec<usize>,
}

impl Node {
    fn step(&mut self, input: Message, output: &mut StdoutLock) -> anyhow::Result<()> {
        match &input.body.payload {
            Payload::Init { node_id, .. } => {
                self.node_id = Some(node_id.clone());
                input.reply(self, Payload::InitOk, output)?;
            },
            Payload::InitOk => bail!("node received init_ok message"),

            Payload::Echo { echo } => {
                let echo = echo.clone();
                input.reply(self, Payload::EchoOk { echo }, output)?;
            },
            Payload::EchoOk { .. } => {},

            Payload::Generate => {
                // node_id's uniqueness is guarenteed by the network
                // msg_id's uniqueness is guarenteed by the node implementation
                let unique_id = format!("{}#{}",
                    self.node_id.as_ref().expect("node to be initialized"),
                    self.msg_id
                );

                input.reply(self, Payload::GenerateOk { id: unique_id }, output)?;
            },
            Payload::GenerateOk { .. } => bail!("node received generate_ok message"),

            Payload::Broadcast { message } => {
                self.messages.push(*message);
                input.reply(self, Payload::BroadcastOk, output)?;
            },
            Payload::BroadcastOk => {},

            Payload::Read => {
                let messages = self.messages.clone();
                input.reply(self, Payload::ReadOk { messages }, output)?;
            },
            Payload::ReadOk { .. } => {},

            Payload::Topology { .. } => {
                input.reply(self, Payload::TopologyOk, output)?;
            },
            Payload::TopologyOk => {},
        };

        return Ok(());
    }
}

fn main() -> anyhow::Result<()> {
    let stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();

    let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();

    let mut node = Node { node_id: None, msg_id: 0, messages: Vec::new() };

    for input in inputs {
        let input = input.context("input from stdin could not be deserialized")?;
        node.step(input, &mut stdout)?;
    }

    return Ok(());
}
