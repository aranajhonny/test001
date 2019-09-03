extern crate fallible_iterator;
extern crate postgres;
extern crate rand;

use rand::Rng;
use std::{thread, time};

use fallible_iterator::FallibleIterator;
use postgres::{Connection, TlsMode};

use serde::{Deserialize, Serialize};
use serde_json::from_str;

pub struct Program {
  id: i32,
}

pub struct Node {
  id: i32,
}

#[derive(Serialize, Deserialize, Debug)]
struct Payload {
  action: String,
  data: Data,
}

#[derive(Serialize, Deserialize, Debug)]
struct Data {
  node_id: i32,
  program_id: i32,
  updated: String,
}

pub fn lock(node: &Node, program: &Program, conn: &Connection) {
  conn
    .execute(
      "SELECT pg_advisory_lock(\"id\") FROM \"programInstance\" WHERE \"id\" = $1",
      &[&program.id],
    )
    .unwrap();
  println!("Node-{} LOCK Program: {}", node.id, program.id)
}

pub fn unlock(node: &Node, program: &Program, conn: &Connection) {
  conn
    .execute(
      "SELECT pg_advisory_unlock(\"id\") FROM \"programInstance\" WHERE \"id\" = $1",
      &[&program.id],
    )
    .unwrap();
  println!("Node-{} UNLOCK Program: {}", node.id, program.id)
}

pub fn parse_str(s: &str) -> &str {
  &s
}

fn main() {
  let mut rng = rand::thread_rng();

  let conn = Connection::connect(
    "postgres://postgres:newpassword@localhost:5432/membrane",
    TlsMode::None,
  )
  .unwrap();

  let notifications = conn.notifications();
  let mut it = notifications.blocking_iter();

  let node = Node {
    id: rng.gen_range(1, 3),
  };

  println!("NODE-ID: {}", node.id);
  conn.execute("LISTEN events", &[]).unwrap();

  while let Ok(Some(notification)) = it.next() {
    let s = parse_str(&notification.payload);

    let v: Payload = from_str(s).unwrap();

    let program = Program {
      id: v.data.program_id,
    };

    if v.data.node_id == node.id {
      lock(&node, &program, &conn);
      // lock manual
      println!("{:?}", v);
      let ten_millis = time::Duration::from_millis(3000);
       // unlock after 3 sec
      thread::sleep(ten_millis);
      unlock(&node, &program, &conn);
    }
  }
}

// #[cfg(test)]
// mod tests {
  // use crate::{unlock , lock, Node, Program};
  // use postgres::{Connection, TlsMode};

//   #[test]
//   fn try_lock_and_unlock() {
//     unimplemented!();
//   }

//   #[test]
//   fn try_unlock() {
//     unimplemented!();
//   }

//   #[test]
//   fn try_listen() {
//     unimplemented!();
//   }

//   #[test]
//   fn try_notify() {
//     unimplemented!();
//   }
// }
