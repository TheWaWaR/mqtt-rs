extern crate mqtt;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate clap;
extern crate uuid;
extern crate time;
extern crate ansi_term;
extern crate rustyline;

use std::net::TcpStream;
use std::io::{self, Write};
use std::collections::{HashMap, LinkedList};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::thread;
use std::process;
use std::time::{Duration};
use std::str;
use std::fmt;

use clap::{App, Arg};
use ansi_term::Colour::{Green, Blue, Red};
use rustyline::error::ReadlineError;
use rustyline::Editor;
use rustyline::completion::{Completer};

use uuid::Uuid;

use mqtt::{Encodable, Decodable, QualityOfService};
use mqtt::packet::*;
use mqtt::control::variable_header::ConnectReturnCode;
use mqtt::{TopicFilter, TopicName};

#[derive(Clone)]
struct MyCompleter {
    commands: Vec<&'static str>
}

impl MyCompleter {
    pub fn new(commands: Vec<&'static str>) -> MyCompleter {
        MyCompleter{commands:commands}
    }
}

impl Completer for MyCompleter {
    fn complete(&self, line: &str, pos: usize) -> rustyline::Result<(usize, Vec<String>)> {
        let mut results: Vec<String> = Vec::new();
        for c in &self.commands {
            if c.starts_with(line) {
                results.push(c.to_string());
            }
        }
        Ok((0, results))
    }
}

struct LocalStatus{
    last_pingresp: Option<time::Tm>,
    counts: HashMap<String, usize>
}

impl fmt::Debug for LocalStatus {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let pingresp_str = match self.last_pingresp {
            Some(v) => format!("{}", v.strftime("%Y-%m-%d %H:%M:%S").unwrap()),
            None => "None".to_string()
        };
        write!(fmt, "LastPingresp=[{}], Counts={:?}", pingresp_str , self.counts)
    }
}

type LocalCount = usize;
type LocalMessages = Option<Vec<PublishPacket>>;

enum Action {
    // Subscribe(TopicName, QualityOfService),
    Publish(String),
    Receive(VariablePacket),
    Status(Sender<LocalStatus>),
    TopicMessageCount(TopicName, Sender<usize>),
    Pull(TopicName, usize, Sender<LocalMessages>)
}


fn generate_client_id() -> String {
    format!("/MQTT/rust/{}", Uuid::new_v4().to_simple_string())
}

fn main() {
    env_logger::init().unwrap();

    let matches = App::new("client")
                      .author("Y. T. Chung <zonyitoo@gmail.com>")
                      .arg(Arg::with_name("SERVER")
                               .short("S")
                               .long("server")
                               .default_value("q.emqtt.com:1883")
                               .takes_value(true)
                               .help("MQTT server address (host:port)"))
                      .arg(Arg::with_name("SUBSCRIBE")
                               .short("s")
                               .long("subscribe")
                               .takes_value(true)
                               .multiple(true)
                               .required(true)
                               .help("Channel filter to subscribe"))
                      .arg(Arg::with_name("USER_NAME")
                               .short("u")
                               .long("username")
                               .takes_value(true)
                               .help("Login user name"))
                      .arg(Arg::with_name("PASSWORD")
                               .short("p")
                               .long("password")
                               .takes_value(true)
                               .help("Password"))
                      .arg(Arg::with_name("CLIENT_ID")
                               .short("i")
                               .long("client-identifier")
                               .takes_value(true)
                               .help("Client identifier"))
                      .get_matches();

    let server_addr = matches.value_of("SERVER").unwrap();
    let client_id = matches.value_of("CLIENT_ID")
                           .map(|x| x.to_owned())
                           .unwrap_or_else(generate_client_id);

    print!("Connecting to {:?} ... ", server_addr);
    io::stdout().flush().unwrap();
    let mut stream = TcpStream::connect(server_addr).unwrap();
    println!("Connected!");

    let keep_alive = 10;
    println!("Client identifier {:?}", client_id);
    let mut conn = ConnectPacket::new("MQTT".to_owned(), client_id.to_owned());
    conn.set_clean_session(true);
    conn.set_keep_alive(keep_alive);
    let mut buf = Vec::new();
    conn.encode(&mut buf).unwrap();
    stream.write_all(&buf[..]).unwrap();

    let connack = ConnackPacket::decode(&mut stream).unwrap();
    trace!("CONNACK {:?}", connack);

    if connack.connect_return_code() != ConnectReturnCode::ConnectionAccepted {
        panic!("Failed to connect to server, return code {:?}",
               connack.connect_return_code());
    }

    let channel_filters: Vec<(TopicFilter, QualityOfService)> =
        matches.values_of("SUBSCRIBE")
               .unwrap()
               .map(|c| {
                   (TopicFilter::new_checked(c.to_string()).unwrap(),
                    QualityOfService::Level0)
               })
               .collect();

    println!("Applying channel filters {:?} ...", channel_filters);
    let sub = SubscribePacket::new(10, channel_filters);
    let mut buf = Vec::new();
    sub.encode(&mut buf).unwrap();
    stream.write_all(&buf[..]).unwrap();

    let channels: Vec<TopicName> = matches.values_of("SUBSCRIBE")
        .unwrap()
        .map(|c| TopicName::new(c.to_string()).unwrap())
        .collect();


    let (tx, rx) = channel::<Action>();
    let cloned_tx = tx.clone();

    // MQTT Background
    let mut cloned_stream = stream.try_clone().unwrap();
    thread::spawn(move || {
        let mut last_ping_time = 0;
        let mut next_ping_time = last_ping_time + (keep_alive as f32 * 0.9) as i64;
        loop {
            let current_timestamp = time::get_time().sec;
            if keep_alive > 0 && current_timestamp >= next_ping_time {
                // println!("Sending PINGREQ to broker");

                let pingreq_packet = PingreqPacket::new();

                let mut buf = Vec::new();
                pingreq_packet.encode(&mut buf).unwrap();
                cloned_stream.write_all(&buf[..]).unwrap();

                last_ping_time = current_timestamp;
                next_ping_time = last_ping_time + (keep_alive as f32 * 0.9) as i64;
                thread::sleep(Duration::new((keep_alive / 2) as u64, 0));
            }
        }
    });

    // Receive packets
    let mut cloned_stream = stream.try_clone().unwrap();
    thread::spawn(move || {
        // println!("Receiving messages!!!!!");
        loop {
            thread::sleep(Duration::new(1, 0));
            let packet = match VariablePacket::decode(&mut cloned_stream) {
                Ok(pk) => pk,
                Err(err) => {
                    error!("Error in receiving packet {:?}", err);
                    continue;
                }
            };
            trace!("PACKET {:?}", packet);
            let _ = cloned_tx.send(Action::Receive(packet.clone()));
        }
    });

    // Process actions
    let mut cloned_stream = stream.try_clone().unwrap();
    thread::spawn(move || {
        // println!("Processing messages!!!!!");
        let mut last_pingresp:Option<time::Tm> = None;
        let mut subscribes: Vec<(TopicFilter, QualityOfService)> = Vec::new();
        let mut mailbox: HashMap<TopicName, LinkedList<PublishPacket>> = HashMap::new();
        // Action process loop
        loop {
            let action = rx.recv().unwrap();
            match action {
                Action::Publish(msg) => {
                    for chan in channels.iter() {
                        let publish_packet = PublishPacket::new(
                            chan.clone(), QoSWithPacketIdentifier::Level0, msg.as_bytes().to_vec());
                        let mut buf = Vec::new();
                        publish_packet.encode(&mut buf).unwrap();
                        cloned_stream.write_all(&buf[..]).unwrap();
                    }
                }
                Action::Receive(packet) => {
                    match &packet {
                        &VariablePacket::PublishPacket(ref packet) => {
                            let topic_name = TopicName::new(packet.topic_name().to_string()).unwrap();
                            mailbox
                                .entry(topic_name.clone())
                                .or_insert(LinkedList::new())
                                .push_back(packet.clone());
                        }
                        &VariablePacket::PingreqPacket(..) => {
                            info!("Ping request recieved");
                            let pingresp = PingrespPacket::new();
                            info!("Sending Ping response {:?}", pingresp);
                            pingresp.encode(&mut cloned_stream).unwrap();
                        }
                        &VariablePacket::PingrespPacket(..) => {
                            // println!("Receiving PINGRESP from broker ..");
                            last_pingresp = Some(time::now());
                        }
                        &VariablePacket::SubscribePacket(ref packet) => {
                            info!("Subscribe packet received: {:?}", packet.payload().subscribes());
                        }
                        &VariablePacket::UnsubscribePacket(ref packet) => {
                            info!("Unsubscribe packet received: {:?}", packet.payload().subscribes());
                        }
                        &VariablePacket::DisconnectPacket(..) => {
                            info!("Disconnecting...");
                            break;
                        }
                        _ => {
                            // Ignore other packets in pub client
                        }
                    }
                }
                Action::Status(sender) => {
                    let mut counts = HashMap::new();
                    for (topic, ref msgs) in &mailbox {
                        counts.insert(topic.to_string(), msgs.len());
                    }
                    let _ = sender.send(LocalStatus{
                        last_pingresp: last_pingresp,
                        counts: counts
                    });
                }
                Action::TopicMessageCount(topic_name, sender) => {
                    if let Some(ref messages) = mailbox.get(&topic_name) {
                        let _ = sender.send(messages.len());
                    } else {
                        let _ = sender.send(0);
                    }
                }
                Action::Pull(topic_name, count, sender) => {
                    if let Some(ref mut messages) = mailbox.get_mut(&topic_name) {
                        let mut msgs = Vec::new();
                        let count = if messages.len() > count { count } else { messages.len() };
                        for _ in 0..count {
                            if let Some(msg) = messages.pop_front() {
                                msgs.push(msg);
                            } else {
                                break;
                            }
                        }
                        let _ = sender.send(Some(msgs));
                    } else {
                        let _ = sender.send(None);
                    }
                }
            }
        }

    });

    let (status_sender, status_receiver) = channel::<LocalStatus>();
    let (count_sender, count_receiver) = channel::<LocalCount>();
    let (message_sender, message_receiver) = channel::<LocalMessages>();

    let commands = vec!["status", "pull", "publish", "exit"];
    let completer = MyCompleter::new(commands.clone());
    let mut rl = Editor::new();
    rl.set_completer(Some(&completer));
    for c in &commands {
        rl.add_history_entry(&c);
    }

    loop {
        io::stdout().flush().unwrap();

        let readline = match rl.readline(format!("{} ", Blue.paint("mqttc>")).as_ref()) {
            Ok(line) => line,
            Err(ReadlineError::Interrupted) => {
                println!("{}", Green.paint("CTRL-C"));
                process::exit(0);
            },
            Err(ReadlineError::Eof) => {
                println!("{}", Green.paint("CTRL-D"));
                process::exit(0);
            },
            Err(err) => {
                println!("{}: {:?}", Red.paint("[Error]"), err);
                process::exit(-1);
            }
        };

        let command = readline.trim();
        let parts: Vec<&str> = command.split_whitespace().collect();
        if let Some(action) = parts.first() {
            match action {
                &"status" => {
                    let _ = tx.send(Action::Status(status_sender.clone()));
                    let status = status_receiver.recv().unwrap();
                    println!("{}: {:?}", Green.paint("[Status]"), status);
                }
                &"pull" => {
                    let topic_name = TopicName::new("abc".to_string()).unwrap();
                    let count: usize = if parts.len() > 1 {
                        parts[1].parse().unwrap_or(1)
                    } else { 1 };
                    let _ = tx.send(Action::Pull(topic_name, count, message_sender.clone()));
                    let rv = message_receiver.recv().unwrap();
                    if let Some(messages) = rv {
                        let mut message_strs: Vec<String> = Vec::new();
                        for msg in messages {
                            message_strs.push(String::from_utf8(msg.payload().to_owned()).unwrap())
                        }
                        println!("{}: {:?}", Green.paint("[Messages]"), message_strs);
                    } else {
                        println!("{}: None", Green.paint("[Messages]"));
                    }
                }
                &"publish" => {
                    if parts.len() > 1 {
                        println!("{}: {}", Green.paint("[Publishing]"), parts[1]);
                        let _ = tx.send(Action::Publish(parts[1].to_string()));
                    } else {
                        println!("{}: message missing!", Red.paint("[Error]"));
                    }
                }
                &"exit" => {
                    println!("{}", Green.paint("[Bye!]"));
                    process::exit(0);
                }
                &"help" => {
                    println!("* {}                     Query mqtt status information\n\
                              * {} [COUNT]               Pull some messages\n\
                              * {} MESSAGE            Publish a message\n\
                              * {}                       Exit this program",
                             Green.bold().paint(commands[0]),
                             Green.bold().paint(commands[1]),
                             Green.bold().paint(commands[2]),
                             Green.bold().paint(commands[3])
                    );
                }
                _ => {
                    println!("{}: {}", Red.paint("[Unknown]"), command);
                }
            }
        }
    }
}
