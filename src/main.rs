use libzmq::{prelude::*, ClientBuilder, TcpAddr, Period};
use std::convert::TryInto;
use serde::{Serialize, Deserialize};
use rumqtt::{MqttClient, MqttOptions, QoS, Notification};
use std::str;
use log::*;
use simplelog::*;
use std::time::Duration;

#[derive(Serialize, Deserialize, Debug)]
struct SendMessage {
    channel_id: u64,
    content: String
}

fn main() {
    if TermLogger::init(LevelFilter::Info, Config::default(), TerminalMode::Mixed).is_err() {
        eprintln!("Failed to create term logger");
        if SimpleLogger::init(LevelFilter::Info, Config::default()).is_err() {
            eprintln!("Failed to create simple logger");
        }
    }

    let general_channel_id = 663862252629393421_u64;
    let hopper_channel_id = 699300787746111528_u64;

    let client = ClientBuilder::new()
        .build()
        .expect("Failed to create ZeroMQ client");
    let addr: TcpAddr = "192.168.0.201:32968".try_into().expect("Failed to parse IP address");
    client.connect(addr).expect("Failed to connect to ZeroMQ host");
    client.set_recv_timeout(Period::Finite(Duration::from_secs(2))).expect("Failed to set timeout");

    let mqtt_options = MqttOptions::new("mqtt_discord_bridge", "mqtt.local", 1883);
    let (mut mqtt_client, notifications) = MqttClient::start(mqtt_options).expect("Failed to connect to MQTT host");
    mqtt_client.subscribe("hopper/telemetry/voltage", QoS::AtMostOnce).expect("Failed to subscribe to topic");
    mqtt_client.subscribe("discord/send/general", QoS::AtMostOnce).expect("Failed to subscribe to topic");

    let mut warning_sent = false;

    for notification in notifications {
        if let Notification::Publish(data) = notification {
            trace!("New message");
            match data.topic_name.as_str() {
                "discord/send/general" => {
                    if let Ok(message_text) = str::from_utf8(&data.payload) {
                        let message = SendMessage {
                            channel_id: general_channel_id,
                            content: message_text.to_owned(),
                        };
        
                        if let Ok(payload) = serde_json::to_string(&message) {
                            if client.send(payload).is_ok() {
                                if client.recv_msg().is_ok() {
                                    trace!("Bridge responded");
                                } else {
                                    error!("No response from bridge");
                                }
                            }
                        } else {
                            error!("Failed to serialize message to JSON");
        
                        }
                    } else {
                        error!("Failed to parse MQTT payload");
                    }
                },
                "hopper/telemetry/voltage" => {
                    if let Ok(message_text) = str::from_utf8(&data.payload) {
                        if let Ok(voltage) = message_text.parse::<f32>() {
                            if voltage < 10.3 && !warning_sent {
                                warning_sent = true;
                                let message = SendMessage {
                                    channel_id: hopper_channel_id,
                                    content: format!("Voltage low: {}", message_text),
                                };
                                if let Ok(payload) = serde_json::to_string(&message) {
                                    if client.send(payload).is_ok() {
                                        if client.recv_msg().is_ok() {
                                            trace!("Bridge responded");
                                        } else {
                                            error!("No response from bridge");
                                        }
                                    }
                                } else {
                                    error!("Failed to serialize message to JSON");
                                }
                            }
                            if voltage > 10.3 {
                                warning_sent = false;
                            }
                        }
                    } else {
                        error!("Failed to parse MQTT payload");
                    }
                },
                _ => {
                    warn!("Unknown topic")
                }
            }
        }
    }

}
