use clap::Args;
use pbf_craft::writers::PbfWriter;

use crate::db::DatabaseReader;

#[derive(Args)]
pub struct ExportCommand {
    /// output path
    #[clap(short, long, value_parser)]
    output: String,

    /// database user
    #[clap(long, value_parser)]
    user: String,

    /// password
    #[clap(long, value_parser)]
    password: String,

    /// the host of the database
    #[clap(long, value_parser)]
    host: String,

    /// the port of the database
    #[clap(long, value_parser, default_value_t = 5432)]
    port: u16,

    /// the database name
    #[clap(long, value_parser)]
    dbname: String,
}

impl ExportCommand {
    pub fn run(self) {
        blue!("Exporting ");
        dark_yellow!(
            "postgres://{}:{}@{}:{}/{}",
            &self.user,
            &self.password,
            &self.host,
            &self.port,
            &self.dbname
        );
        blue!(" to ");
        dark_yellow!("{}", self.output);
        println!(" ...");

        let db_reader =
            DatabaseReader::new(self.host, self.port, self.dbname, self.user, self.password);
        let mut writer = PbfWriter::from_path(&self.output, true).unwrap();
        db_reader
            .read(|el_container| writer.write(el_container).expect("write error"))
            .expect("read failed");
        writer.finish().expect("finished error");
    }
}
