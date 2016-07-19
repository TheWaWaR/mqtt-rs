extern crate mqtt;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate clap;
extern crate uuid;
extern crate time;

use std::net::TcpStream;
use std::io::{self, Write};
use std::collections::{HashMap, LinkedList};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::thread;
use std::time::Duration;
use std::str;

use clap::{App, Arg};

use uuid::Uuid;

use mqtt::{Encodable, Decodable, QualityOfService};
use mqtt::packet::*;
use mqtt::control::variable_header::ConnectReturnCode;
use mqtt::{TopicFilter, TopicName};


type LocalStatus = HashMap<String, usize>;
type LocalCount = usize;
type LocalMessages = Option<Vec<PublishPacket>>;

enum Action {
    // Subscribe(TopicName, QualityOfService),
    // Publish(TopicName, QualityOfService),
    Receive(VariablePacket),
    Status(Sender<LocalStatus>),
    TopicMessageCount(TopicName, Sender<usize>),
    PullMessages(TopicName, usize, Sender<LocalMessages>)
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
                println!("Sending PINGREQ to broker");

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
        println!("Receiving messages!!!!!");
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
        println!("Processing messages!!!!!");
        let mut subscribes: Vec<(TopicFilter, QualityOfService)> = Vec::new();
        let mut mailbox: HashMap<TopicName, LinkedList<PublishPacket>> = HashMap::new();
        // Action process loop
        loop {
            let action = rx.recv().unwrap();
            match action {
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
                            println!("Receiving PINGRESP from broker ..");
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
                    let mut result = HashMap::new();
                    for (topic, ref msgs) in &mailbox {
                        result.insert(topic.to_string(), msgs.len());
                    }
                    let _ = sender.send(result);
                }
                Action::TopicMessageCount(topic_name, sender) => {
                    if let Some(ref messages) = mailbox.get(&topic_name) {
                        let _ = sender.send(messages.len());
                    } else {
                        let _ = sender.send(0);
                    }
                }
                Action::PullMessages(topic_name, count, sender) => {
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

    let stdin = io::stdin();
    loop {
        io::stdout().flush().unwrap();

        let mut line = String::new();
        stdin.read_line(&mut line).unwrap();

        let command = line.trim();
        println!("[Input]: {}", command);
        let parts: Vec<&str> = command.split_whitespace().collect();
        if let Some(action) = parts.first() {
            match action {
                &"status" => {
                    let _ = tx.send(Action::Status(status_sender.clone()));
                    let status = status_receiver.recv().unwrap();
                    println!("[Status]: {:?}", status);
                }
                &"messages" => {
                    let topic_name = TopicName::new("abc".to_string()).unwrap();
                    let _ = tx.send(Action::PullMessages(topic_name, 1, message_sender.clone()));
                    let messages = message_receiver.recv().unwrap();
                    println!("[Message]: {:?}", messages);
                }
                _ => {
                    println!("[Unknown command]: {}", command);
                }
            }
        } else {
            println!("[Empty command]");
        }
    }
}
