// Fixture adapter appended to unmodified sysinfo 0.39.6 linux/cgroup.rs.
// Provides the same whole-file UTF-8 helper without unrelated platform modules.
mod sys {
    pub mod utils {
        pub fn get_all_utf8_data<P: AsRef<std::path::Path>>(
            path: P, _: usize,
        ) -> std::io::Result<String> {
            std::fs::read_to_string(path)
        }
    }
}

pub struct CGroupLimits {
    pub total_memory: u64,
    pub free_memory: u64,
    pub free_swap: u64,
    pub rss: u64,
}

fn main() {
    let input = std::fs::read_to_string(std::env::args().nth(1).unwrap()).unwrap();
    for line in input.lines() {
        let columns: Vec<_> = line.split('\t').collect();
        let root = std::path::Path::new(columns[1]);
        let base = root.join(columns[2]);
        let context = CGroupLimitsContext {
            mem_total: columns[3].parse().unwrap(), swap_total: 0, swap_free: 0,
        };
        let result = if columns[0] == "v2" {
            v2_limits(&base, root, context)
        } else {
            v1_limits(&base, root, context)
        };
        match result {
            Some(limits) => println!("{},{}", limits.total_memory, limits.free_memory),
            None => println!("None"),
        }
    }
}
