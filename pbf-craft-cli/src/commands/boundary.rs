use clap::Args;
use geo::{self, ConvexHull, Geometry, Polygon};
use geojson::Value;

use pbf_craft::pbf::readers::PbfReader;

#[derive(Args)]
pub struct BoundaryCommand {
    /// file path
    #[clap(short, long, value_parser)]
    file: String,
}

impl BoundaryCommand {
    pub fn run(self) {
        let mut reader =
            PbfReader::from_path(&self.file).expect(&format!("No such file: {}", self.file));

        let mut polygons: Vec<Polygon> = Vec::new();
        while let Some(blob_data) = reader.read_next_blob() {
            if blob_data.nodes.len() > 0 {
                let points: Vec<geo::Point> = blob_data
                    .nodes
                    .into_iter()
                    .map(|node| {
                        let x: f64 = node.longitude as f64 / 1000000000f64;
                        let y: f64 = node.latitude as f64 / 1000000000f64;
                        geo::Point::new(x, y)
                    })
                    .collect();
                let multi_points = geo::MultiPoint::new(points);
                polygons.push(multi_points.convex_hull());
            }
        }
        let multi_polygons = geo::MultiPolygon::new(polygons);
        let boundary = multi_polygons.convex_hull();
        let geometry: Geometry<f64> = boundary.into();
        let geojson = Value::from(&geometry);
        dark_yellow_ln!("---------");
        println!("{}", geojson.to_string());
    }
}
