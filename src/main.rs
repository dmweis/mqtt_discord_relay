use libzmq::{prelude::*, ClientBuilder, TcpAddr, Client};
use std::convert::TryInto;
use serde::{Serialize, Deserialize};
use rumqtt::{MqttClient, MqttOptions, QoS, Notification, ReconnectOptions};
use std::str;
use log::*;
use simplelog::*;
use std::time::Duration;
use std::thread::{sleep, self};

#[derive(Serialize, Deserialize, Debug)]
struct RelayMessage {
    channel_id: u64,
    content: String
}

#[derive(Serialize, Deserialize, Debug)]
enum MessageWrapper {
    Message(RelayMessage),
    KeepAlive
}

fn start_zmq_rcv_thread(zmq_client: Client, mut mqtt_client: MqttClient) {
    thread::spawn(move || {
        let general_channel_id = 663862252629393421_u64;
        let hopper_channel_id = 699300787746111528_u64;
        loop {
            let payload = zmq_client.recv_msg().unwrap();
            let message: MessageWrapper = serde_json::from_slice(payload.as_bytes()).unwrap();
            if let MessageWrapper::Message(message) = message {
                mqtt_client.publish(
                    format!("discord/receive/{}", message.channel_id),
                    QoS::AtMostOnce,
                    false,
                    message.content.clone()
                ).unwrap();
                if message.channel_id == general_channel_id {
                    mqtt_client.publish(
                        "discord/receive/general",
                        QoS::AtMostOnce,
                        false,
                        message.content.clone()
                    ).unwrap();
                }
                if message.channel_id == hopper_channel_id {
                    mqtt_client.publish(
                        "discord/receive/hopper",
                        QoS::AtMostOnce,
                        false,
                        message.content.clone()
                    ).unwrap();
                }
            }
        }
    });
}

fn send_to_discord(zmq_client: &Client, channel_id: u64, text: String) {
    let message = MessageWrapper::Message(RelayMessage {
        channel_id: channel_id,
        content: text,
    });
    if let Ok(payload) = serde_json::to_string(&message) {
        if zmq_client.send(payload).is_ok() {
            trace!("Message sent over ZMQ");
        }
    } else {
        error!("Failed to serialize message to JSON");
    }
}

fn main() {
    let config = ConfigBuilder::new()
        .add_filter_allow_str("mqtt_discord_relay")
        .build();
    if TermLogger::init(LevelFilter::Info, config.clone(), TerminalMode::Mixed).is_err() {
        eprintln!("Failed to create term logger");
        if SimpleLogger::init(LevelFilter::Info, config).is_err() {
            eprintln!("Failed to create simple logger");
        }
    }

    let general_channel_id = 663862252629393421_u64;
    let hopper_channel_id = 699300787746111528_u64;

    let client = ClientBuilder::new()
        .build()
        .expect("Failed to create ZeroMQ client");
    let addr: TcpAddr = "127.0.0.1:32968".try_into().expect("Failed to parse IP address");
    client.connect(addr).expect("Failed to connect to ZeroMQ host");

    let client_copy = client.clone();
    thread::spawn(move || {
        loop {
            client_copy.send(serde_json::to_string(&MessageWrapper::KeepAlive).unwrap()).unwrap();
            sleep(Duration::from_secs(5));
        }
    });

    // client.set_recv_timeout(Period::Finite(Duration::from_secs(2))).expect("Failed to set timeout");

    let mqtt_options = MqttOptions::new("KNFKJAHSFUHAJKFH", "mqtt.local", 1883)
        .set_reconnect_opts(ReconnectOptions::Always(5));
    let (mut mqtt_client, notifications) = MqttClient::start(mqtt_options).expect("Failed to connect to MQTT host");
    mqtt_client.subscribe("hopper/telemetry/voltage", QoS::AtMostOnce).expect("Failed to subscribe to topic");
    mqtt_client.subscribe("discord/send/general", QoS::AtMostOnce).expect("Failed to subscribe to topic");
    mqtt_client.subscribe("hopper/telemetry/warning_voltage", QoS::AtMostOnce).expect("Failed to subscribe to topic");


    start_zmq_rcv_thread(client.clone(), mqtt_client.clone());

    let mut warning_sent = false;
    let mut warning_voltage = 10.5;

    for notification in notifications {
        if let Notification::Publish(data) = notification {
            trace!("New message");
            match data.topic_name.as_str() {
                "discord/send/general" => {
                    if let Ok(message_text) = str::from_utf8(&data.payload) {
                        send_to_discord(&client, general_channel_id, message_text.to_owned());
                    } else {
                        error!("Failed to parse MQTT payload");
                    }
                },
                "hopper/telemetry/voltage" => {
                    if let Ok(message_text) = str::from_utf8(&data.payload) {
                        if let Ok(voltage) = message_text.parse::<f32>() {
                            if voltage < warning_voltage && !warning_sent {
                                warning_sent = true;
                                send_to_discord(&client, hopper_channel_id, format!("Voltage low: {}", voltage));
                            }
                            if voltage > warning_voltage {
                                if warning_sent {
                                    send_to_discord(&client, hopper_channel_id, format!("Voltage good: {}", voltage));
                                }
                                warning_sent = false;
                            }
                        }
                    } else {
                        error!("Failed to parse MQTT payload");
                    }
                },
                "hopper/telemetry/warning_voltage" => {
                    if let Ok(message_text) = str::from_utf8(&data.payload) {
                        if let Ok(voltage) = message_text.parse::<f32>() {
                            warning_voltage = voltage;
                            info!("Set new warning voltage of {}", voltage);
                        }
                    }
                }
                _ => {
                    warn!("Unknown topic")
                }
            }
        } else if let Notification::Disconnection = notification {
            warn!("Client disconnected from MQTT");
        } else if let Notification::Reconnection = notification {
            mqtt_client.subscribe("hopper/telemetry/voltage", QoS::AtMostOnce).expect("Failed to subscribe to topic");
            mqtt_client.subscribe("discord/send/general", QoS::AtMostOnce).expect("Failed to subscribe to topic");
            mqtt_client.subscribe("hopper/telemetry/warning_voltage", QoS::AtMostOnce).expect("Failed to subscribe to topic");
            warn!("Client reconnected to MQTT");
        }
    }

}
