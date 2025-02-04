use crate::cmd::{new_config_cmd, new_start_cmd, new_stop_cmd};

use crate::configure::{generate_default_config, set_config_file_path};
use crate::configure::{get_config, get_config_file_path, get_current_config_yml, set_config};

use crate::httpserver;
use crate::resources::init_resources;
use crate::tasks::{
    init_tasks_status_server, GLOBAL_TASK_JOINSET, GLOBAL_TASK_RUNTIME, GLOBAL_TASK_STOP_MARK_MAP,
};
use clap::{Arg, ArgAction, ArgMatches};
use fork::{daemon, Fork};
use lazy_static::lazy_static;
use signal_hook::consts::{SIGTERM, TERM_SIGNALS};
use signal_hook::iterator::exfiltrator::WithOrigin;
use signal_hook::iterator::SignalsInfo;
use std::net::{self, IpAddr};
use std::process::{exit, Command};
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::{env, fs, thread};
use sysinfo::{Pid, RefreshKind, System};
use tokio::net::TcpListener;
use tokio::runtime::{self, Runtime};

lazy_static! {
    static ref CLIAPP: clap::Command = clap::Command::new("serverframe-rs")
        .version("1.0")
        .author("Shiwen Jia. <jiashiwen@gmail.com>")
        .about("RustBoot")
        .arg_required_else_help(true)
        .arg(
            Arg::new("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .help("Sets a custom config file")
        )
        .subcommand(
            new_start_cmd().arg(
                Arg::new("daemon")
                    .short('d')
                    .long("daemon")
                    .action(ArgAction::SetTrue)
                    .help("run as daemon")
            )
        )
        .subcommand(new_stop_cmd())
        .subcommand(new_config_cmd());
    // static ref SUBCMDS: Vec<SubCmd> = subcommands();
}

pub fn run_app() {
    let matches = CLIAPP.clone().get_matches();
    cmd_match(&matches);
}

// pub fn run_from(args: Vec<String>) {
//     match clap::Command::try_get_matches_from(CLIAPP.to_owned(), args.clone()) {
//         Ok(matches) => {
//             cmd_match(&matches);
//         }
//         Err(err) => {
//             err.print().expect("Error writing Error");
//         }
//     };
// }

// 获取全部子命令，用于构建commandcompleter
// pub fn all_subcommand(app: &clap::Command, beginlevel: usize, input: &mut Vec<SubCmd>) {
//     let nextlevel = beginlevel + 1;
//     let mut subcmds = vec![];
//     for iterm in app.get_subcommands() {
//         subcmds.push(iterm.get_name().to_string());
//         if iterm.has_subcommands() {
//             all_subcommand(iterm, nextlevel, input);
//         } else {
//             if beginlevel == 0 {
//                 all_subcommand(iterm, nextlevel, input);
//             }
//         }
//     }
//     let subcommand = SubCmd {
//         level: beginlevel,
//         command_name: app.get_name().to_string(),
//         subcommands: subcmds,
//     };
//     input.push(subcommand);
// }

// pub fn get_command_completer() -> CommandCompleter {
//     CommandCompleter::new(SUBCMDS.to_vec())
// }

// fn subcommands() -> Vec<SubCmd> {
//     let mut subcmds = vec![];
//     all_subcommand(&CLIAPP.clone().borrow(), 0, &mut subcmds);
//     subcmds
// }

fn cmd_match(matches: &ArgMatches) {
    if let Some(c) = matches.get_one::<String>("config") {
        set_config_file_path(c.to_string());
        set_config(&get_config_file_path());
    } else {
        set_config("");
    }

    if let Some(ref matches) = matches.subcommand_matches("start") {
        if matches.get_flag("daemon") {
            let args: Vec<String> = env::args().collect();
            if let Ok(Fork::Child) = daemon(true, true) {
                // Start child thread
                let mut cmd = Command::new(&args[0]);
                for idx in 1..args.len() {
                    let arg = args.get(idx).expect("get cmd arg error!");
                    // remove start as daemon variable
                    // 去除后台启动参数,避免重复启动
                    if arg.eq("-d") || arg.eq("-daemon") {
                        continue;
                    }
                    cmd.arg(arg);
                }

                let child = cmd.spawn().expect("Child process failed to start.");
                fs::write("pid", child.id().to_string()).expect("Write pid file error!");
            }
            println!("{}", "daemon mod");
            std::process::exit(0);
        }

        let banner = r" 
        _____                                                                       _____ 
       ( ___ )---------------------------------------------------------------------( ___ )
        |   |                                                                       |   | 
        |   |  _______  __   __       _______    .______    __  .______    _______  |   | 
        |   | |   ____||  | |  |     |   ____|   |   _  \  |  | |   _  \  |   ____| |   | 
        |   | |  |__   |  | |  |     |  |__      |  |_)  | |  | |  |_)  | |  |__    |   | 
        |   | |   __|  |  | |  |     |   __|     |   ___/  |  | |   ___/  |   __|   |   | 
        |   | |  |     |  | |  `----.|  |____    |  |      |  | |  |      |  |____  |   | 
        |   | |__|     |__| |_______||_______|   | _|      |__| | _|      |_______| |   | 
        |   |      _______. _______ .______     ____    ____  _______ .______       |   | 
        |   |     /       ||   ____||   _  \    \   \  /   / |   ____||   _  \      |   | 
        |   |    |   (----`|  |__   |  |_)  |    \   \/   /  |  |__   |  |_)  |     |   | 
        |   |     \   \    |   __|  |      /      \      /   |   __|  |      /      |   | 
        |   | .----)   |   |  |____ |  |\  \----.  \    /    |  |____ |  |\  \----. |   | 
        |   | |_______/    |_______|| _| `._____|   \__/     |_______|| _| `._____| |   | 
        |___|                                                                       |___| 
       (_____)---------------------------------------------------------------------(_____)";

        println!("{}", banner);
        println!("current pid is:{}", std::process::id());

        //启动公共 tokio runtime
        GLOBAL_TASK_RUNTIME.block_on(async {
            log::info!("global runtime start!");
            log::info!(
                "global task joinset is empty:{}",
                GLOBAL_TASK_JOINSET.read().await.is_empty()
            );
        });

        // 初始化外部资源
        let rt = Runtime::new().unwrap();
        rt.block_on(async { init_resources().await.unwrap() });

        rt.spawn(async move { init_tasks_status_server().await });

        // let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        // let async_http_server = async {
        //     let config = get_config().unwrap();
        //     let bind = config.http.bind;
        //     let port = config.http.port;

        //     let mut http_server = httpserver::HttpServer::default().await;
        //     let ip = IpAddr::from_str(&bind).unwrap();
        //     let addr = net::SocketAddr::from((ip, port));

        //     http_server.listener = TcpListener::bind(addr).await.unwrap();

        //     let http_handler = http_server.run().await;
        //     let _http = tokio::join!(http_handler);
        // };

        let async_http_server = async {
            let config = get_config().unwrap();
            let bind = config.http.bind;
            let port = config.http.port;

            let mut http_server = httpserver::HttpServer::default().await;
            let ip = IpAddr::from_str(&bind).unwrap();
            let addr = net::SocketAddr::from((ip, port));

            http_server.listener = TcpListener::bind(addr).await.unwrap();

            let http_handler = http_server.run().await;
            let _http = tokio::join!(http_handler);
        };

        let thread_http = thread::spawn(|| {
            // let rt = Runtime::new().unwrap();
            let rt = runtime::Builder::new_multi_thread()
                .worker_threads(num_cpus::get())
                .enable_all()
                .max_io_events_per_tick(32)
                .build()
                .unwrap();
            rt.block_on(async_http_server);
        });

        let thread_signale = thread::spawn(|| {
            // 添加signal处理机制
            let mut sigs = vec![];
            sigs.extend(TERM_SIGNALS);
            let mut signals = SignalsInfo::<WithOrigin>::new(&sigs).unwrap();
            for info in &mut signals {
                // Will print info about signal + where it comes from.
                log::info!("Received a signal {:?}", info);
                for kv in GLOBAL_TASK_STOP_MARK_MAP.iter() {
                    kv.store(true, std::sync::atomic::Ordering::SeqCst);
                }
                // GLOBAL_TASK_STOP_MARK_MAP.store(true, std::sync::atomic::Ordering::SeqCst);
                match info.signal {
                    SIGTERM => {
                        println!("kill !");
                        exit(1);
                    }
                    term_sig => {
                        eprintln!("Terminating......");
                        // do some before exit
                        exit(0)
                    }
                }
            }
        });
        thread_http.join().unwrap();
        thread_signale.join().unwrap();
    }

    if let Some(ref _matches) = matches.subcommand_matches("stop") {
        println!("server stopping...");

        // let sys = System::new_with_specifics(RefreshKind::everything().without_disks_list());
        let sys =
            System::new_with_specifics(RefreshKind::everything().without_cpu().without_memory());
        let pidstr = String::from_utf8(fs::read("pid").unwrap()).unwrap();
        let pid = Pid::from_str(pidstr.as_str()).unwrap();

        if let Some(p) = sys.process(pid) {
            println!("terminal process: {:?}", p.pid());
        } else {
            println!("Server not run!");
            return;
        };
        Command::new("kill")
            .args(["-15", pidstr.as_str()])
            .output()
            .expect("failed to execute process");
    }

    if let Some(config) = matches.subcommand_matches("config") {
        if let Some(_show) = config.subcommand_matches("show") {
            let yml = get_current_config_yml();
            match yml {
                Ok(str) => {
                    println!("{}", str);
                }
                Err(e) => {
                    eprintln!("{}", e);
                }
            }
        }

        if let Some(gen_config) = config.subcommand_matches("gendefault") {
            let mut file = String::from("");
            if let Some(path) = gen_config.get_one::<&str>("filepath") {
                file.push_str(path);
            } else {
                file.push_str("config_default.yml")
            }
            if let Err(e) = generate_default_config(file.as_str()) {
                log::error!("{}", e);
                return;
            };
            println!("{} created!", file);
        }
    }
}
