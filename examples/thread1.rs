use anyhow::{anyhow, Result};
use std::{sync::mpsc, thread, time::Duration};

const NUM_PRODUCERS: usize = 4;

#[allow(dead_code)]
#[derive(Debug)]
struct Msg {
    idx: usize,
    value: usize,
}

fn main() -> Result<()> {
    // mpsc: multi-producer, single cosumer
    let (tx, rx) = mpsc::channel();
    // 创建 producers
    for i in 0..NUM_PRODUCERS {
        let tx = tx.clone();
        thread::spawn(move || producer(i, tx));
    }
    // 释放tx，否则rx就没办法结束
    drop(tx);

    // 创建 consumers
    let consumer = thread::spawn(move || {
        for msg in rx {
            println!("consumer: {:?}", msg)
        }
        println!("consumer exit");
        42
    });

    let secret = consumer
        .join()
        .map_err(|e| anyhow!("Thread join error: {:?}", e))?;

    println!("secret: {}", secret);

    Ok(())
}

fn producer(idx: usize, tx: mpsc::Sender<Msg>) -> Result<()> {
    loop {
        let value = rand::random::<usize>();
        tx.send(Msg::new(idx, value))?;
        let sleep_time = rand::random::<u8>() as u64 * 10;
        thread::sleep(Duration::from_millis(sleep_time));
        if rand::random::<u8>() % 3 == 0 {
            println!("producer {} exit", idx);
            break;
        }
    }
    Ok(())
}

impl Msg {
    fn new(idx: usize, value: usize) -> Self {
        Self { idx, value }
    }
}
