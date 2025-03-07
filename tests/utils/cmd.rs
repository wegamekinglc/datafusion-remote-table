use std::process::Command;

pub fn run_command(mut cmd: Command, desc: impl ToString) {
    let desc = desc.to_string();
    println!("Starting to {}, command: {:?}", &desc, cmd);
    let exit = cmd.status().unwrap();
    if exit.success() {
        println!("{} succeed!", desc)
    } else {
        panic!("{} failed: {:?}", desc, exit);
    }
}

pub fn get_cmd_output_result(mut cmd: Command, desc: impl ToString) -> Result<String, String> {
    let desc = desc.to_string();
    println!("Starting to {}, command: {:?}", &desc, cmd);
    let result = cmd.output();
    match result {
        Ok(output) => {
            if output.status.success() {
                println!("{} succeed!", desc);
                Ok(String::from_utf8(output.stdout).unwrap())
            } else {
                Err(format!("{} failed with rc: {:?}", desc, output.status))
            }
        }
        Err(err) => Err(format!("{} failed with error: {}", desc, { err })),
    }
}

pub fn get_cmd_output(cmd: Command, desc: impl ToString) -> String {
    let result = get_cmd_output_result(cmd, desc);
    match result {
        Ok(output_str) => output_str,
        Err(err) => panic!("{}", err),
    }
}