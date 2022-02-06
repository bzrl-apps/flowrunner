use tokio::time::{sleep, Duration};

use rdkafka::{
    consumer::{BaseConsumer, DefaultConsumerContext},
    client::DefaultClientContext,
    admin::{
        AdminClient, AdminOptions, NewTopic,
        TopicReplication,
    },
    //metadata::Metadata,
    ClientConfig, producer::FutureProducer,
};

use sqlx::postgres::PgPoolOptions;

use log::info;

pub fn init_log() {
    let _ = env_logger::builder().is_test(true).try_init();
}

pub fn create_config(brokers: &str) -> ClientConfig {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", brokers);
    config
}

pub fn create_admin_client(brokers: &str) -> AdminClient<DefaultClientContext> {
    create_config(brokers)
        .create()

        .expect("admin client creation failed")
}

pub fn create_consumer_client(brokers: &str, group_id: &str) -> BaseConsumer<DefaultConsumerContext> {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", brokers);
    config.set("group.id", group_id);
    config.set("auto.offset.reset", "earliest");
    config.create()
        .expect("consumer creation failed")
}

pub fn create_producer_client(brokers: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create().unwrap()
}

pub async fn init_kafka(topics: &[&str]) {
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(1)));
    let admin_client = create_admin_client("localhost:9092");
    //let topics: &[&str] = &["topic1", "topic2", "topic_output"];

    info!("Deleting topics...");

    admin_client.delete_topics(topics, &opts)
        .await
        .expect("topic deletion failed");

    sleep(Duration::from_millis(5000)).await;

    for t in topics.iter() {
        info!("Creating topic: {}", t);
        NewTopic::new(t, 1, TopicReplication::Fixed(1));
        sleep(Duration::from_millis(5000)).await;
    }
}

pub async fn init_db() {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://flowrunner:flowrunner@localhost:5432/flowrunner")
        .await
        .unwrap();

    sqlx::query(r#"DROP TABLE IF EXISTS users;"#)
        .execute(&pool)
        .await
        .unwrap();

    sqlx::query(r#"
CREATE TABLE IF NOT EXISTS users (
id serial PRIMARY KEY,
username VARCHAR ( 50 ) UNIQUE NOT NULL,
password character varying(64)NOT NULL,
enabled boolean,
age integer,
created_at timestamp with time zone DEFAULT now()
);"#)
        .execute(&pool)
        .await
        .unwrap();
}
