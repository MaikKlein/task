#![feature(associated_consts, unboxed_closures, fn_traits, type_macros, fnbox, thread_local)]
extern crate arrayvec;
extern crate coroutine;
extern crate rand;
extern crate typenum;
#[macro_use]
extern crate generic_array;
extern crate num;
#[macro_use]
extern crate lazy_static;
use num::{Float, Zero};
use typenum::*;
use generic_array::*;
use std::ops::{Add, Sub, Mul};
use std::boxed::FnBox;
use coroutine::asymmetric::Coroutine;
use std::iter::*;
use std::cell::*;
use std::sync::mpsc::*;
use std::thread;
use std::thread::JoinHandle;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use std::mem::uninitialized;
use arrayvec::{ArrayVec, Array};
use num::Num;

use std::ptr::*;

lazy_static! {
    pub static ref TASK: TaskPool = TaskPool::new(3);
}

thread_local!(static FIBER: Cell<*mut coroutine::asymmetric::Handle> = Cell::new(std::ptr::null_mut()));
enum Message {
    Task(Box<FnBox(&mut Coroutine) + Send + 'static>),
}
struct LocalTaskQueue {
    work: Vec<coroutine::asymmetric::Handle>,
    recv: Receiver<Message>,
}
impl LocalTaskQueue {
    fn new(r: Receiver<Message>) -> LocalTaskQueue {
        return LocalTaskQueue {
            work: vec![],
            recv: r,
        };
    }
    fn start(&mut self) {
        loop {
            match self.recv.try_recv() {
                Ok(value) => {
                    match value {
                        Message::Task(f) => {
                            let mut c = Coroutine::spawn(|me| f.call_box((me,)));
                            self.work.push(c);
                        }
                    }
                }
                Err(e) => {
                    match e {
                        TryRecvError::Empty => (),
                        TryRecvError::Disconnected => {
                            break;
                        }
                    }
                }
            }
            for t in self.work.iter_mut() {
                FIBER.with(|f| {
                    f.set(t);
                });
                t.next();
                FIBER.with(|f| {
                    f.set(std::ptr::null_mut());
                });
            }
            self.work.retain(|w| {
                return !w.is_finished();
            });
        }
    }
}

use rand::ThreadRng;
use rand::distributions::{Sample, IndependentSample, Range};
use std::sync::Mutex;
pub struct TaskPool {
    threads: Vec<(JoinHandle<()>, Mutex<Sender<Message>>)>,
    range: Range<usize>,
}
impl TaskPool {
    fn new(size: usize) -> TaskPool {
        let mut v = Vec::with_capacity(size);
        for i in 0..size {
            let (sender, recv) = channel::<Message>();
            v.insert(i,
                     (thread::Builder::new()
                         .name(i.to_string())
                         .spawn(move || {
                             let mut local_queue = LocalTaskQueue::new(recv);
                             local_queue.start();
                         })
                         .unwrap(),
                      Mutex::new(sender)));
        }
        let range = Range::new(0, v.len());
        return TaskPool {
            range: range,
            threads: v,
        };
    }
    fn send(&self, msg: Message) {
        let mut rng = rand::thread_rng();
        self.threads[self.range.ind_sample(&mut rng)].1.lock().unwrap().send(msg);
    }

    fn submit_impl<'r, R: Send + 'static, F: FnOnce() -> R + Send + 'static>(&'r self,
                                                                             f: F)
                                                                             -> Future<R> {
        let (send, recv) = channel::<R>();
        self.send(Message::Task(Box::new(move |me: &mut Coroutine| {
            let r = f();
            send.send(r);
        })));
        Future::new(recv)
    }
    fn submit<R: Send + 'static, F: FnOnce() -> R + Send + 'static>(f: F) -> Future<R> {
        TASK.submit_impl(f)
    }
}
struct Future<T> {
    receiver: Receiver<T>,
}
impl<T> Future<T> {
    fn new(receiver: Receiver<T>) -> Self {
        Future { receiver: receiver }
    }

    fn await(&self) -> T {
        let mut fiber = FIBER.with(|f| {
            return f.get();
        });
        let is_fiber = fiber != std::ptr::null_mut();
        if is_fiber {
            loop {
                let r = self.receiver.try_recv();
                if r.is_ok() {
                    return r.unwrap();
                }
                unsafe {
                    (*fiber).resume(0);
                }
            }
        } else {
            return self.receiver.recv().unwrap();
        }
    }
}
fn main() {
    let res = TaskPool::submit(|| {
        println!("Before long running task");
        let r = TaskPool::submit(|| {
            std::thread::sleep(Duration::from_secs(10));
            return 42;
        });
        // Waits for the long running task to complete, does not block other tasks!
        println!("After long running task {}", r.await());
        42
    });
    for i in 0..20 {
        TaskPool::submit(move || println!("Another Task {}", i));
    }
    println!("{}", res.await());
}
