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

use sqlx::postgres::{PgPool, PgPoolOptions};

use rocksdb::{DB, ColumnFamilyDescriptor};

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

    info!("Deleting topics... {:?}", topics);

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

pub async fn init_db(tables: &[&str]) -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://flowrunner:flowrunner@localhost:5432/flowrunner")
        .await
        .unwrap();

    info!("Deleting tables... {:?}", tables);
    for t in tables.iter() {
        sqlx::query(format!("DROP TABLE IF EXISTS {};", t).as_str())
            .execute(&pool)
            .await
            .unwrap();
    }

    pool
}

pub fn cleanup_rocksdb(path: &str, namespaces: &[&str]) {
    //let mut cfs: Vec<ColumnFamilyDescriptor> = Vec::new();
    let options = rocksdb::Options::default();
    // list existing ColumnFamilies in the given path. returns Err when no DB exists.
    let cfs_old = rocksdb::DB::list_cf(&options, path).unwrap_or_default();
    if !cfs_old.is_empty() {
        let mut instance = rocksdb::DB::open_cf(&options, path, cfs_old).unwrap();
        for cf in namespaces.iter() {
            info!("Deleting namespace: {}", cf);
            // open a DB with specifying ColumnFamilies
            let _ = instance.drop_cf(cf);
        }
    }

    //for cf in namespaces.iter() {
        //info!("Add namespaces to create: {cf}");
        //cfs.push(ColumnFamilyDescriptor::new(cf.to_owned(), rocksdb::Options::default()));
    //}

    //info!("Open a rocksdb at the path: {path}");
    //DB::open_cf_descriptors_with_ttl(&rocksdb::Options::default(), path, cfs, Duration::from_secs(ttl)).unwrap()
}

pub fn open_rocksdb(path: &str, namespaces: &[&str], ttl: u64) -> DB {
    let mut cfs: Vec<ColumnFamilyDescriptor> = Vec::new();
    let options = rocksdb::Options::default();

    for cf in namespaces.iter() {
        info!("Add namespaces to create: {cf}");
        cfs.push(ColumnFamilyDescriptor::new(cf.to_owned(), rocksdb::Options::default()));
    }

    info!("Open a rocksdb at the path: {path}");
    if ttl > 0 {
        return DB::open_cf_descriptors_with_ttl(&options, path, cfs, Duration::from_secs(ttl)).unwrap()
    }

    DB::open_cf_descriptors(&options, path, cfs).unwrap()
}
