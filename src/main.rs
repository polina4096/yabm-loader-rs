use std::num::NonZeroU32;

use sqlx::MySqlPool;
use tokio::fs::{File, self};
use async_zip::tokio::read::seek::ZipFileReader;

fn compare_write(a: &mut String, b: &str) {
    if a != b {
        a.clear();
        a.push_str(b);
    }
}

#[derive(Debug, Clone)]
struct Beatmap {
    id             : Option<NonZeroU32>,
    title          : String,
    title_unicode  : String,
    artist         : String,
    artist_unicode : String,
    mapper         : String,
}

impl Default for Beatmap {
    fn default() -> Self {
        Self {
            id              : Default::default(),
            title           : Default::default(),
            title_unicode   : Default::default(),
            artist          : Default::default(),
            artist_unicode  : Default::default(),
            mapper          : Default::default(),
        }
    }
}


#[derive(Debug, Clone)]
struct Difficulty<'a> {
    id   : Option<NonZeroU32>,
    name : &'a str,
}

impl<'a> Default for Difficulty<'a> {
    fn default() -> Self {
        Self {
            id   : Default::default(),
            name : Default::default(),
        }
    }
}

fn extract_metadata<'a>(str: &'a str, beatmap: &mut Beatmap) -> Option<Difficulty<'a>> {
    let Some(meta) = str.find("[Metadata]").and_then(|n| Some(n + "[Metadata]".len())) else { return None };
    let Some(next) = str[meta ..].find("[") else { return None };
    let meta = &str[meta .. meta + next];

    let mut data = Difficulty::default();

    for line in meta.lines() {
        let line = line.trim();
        if line.is_empty() { continue };
        
        let Some((key, value)) = line.split_once(':') else { continue };
        match key {
            "Title"         => { compare_write(&mut beatmap.title, value); }
            "TitleUnicode"  => { compare_write(&mut beatmap.title_unicode, value); }
            "Artist"        => { compare_write(&mut beatmap.artist, value); }
            "ArtistUnicode" => { compare_write(&mut beatmap.artist_unicode, value); }
            "Creator"       => { compare_write(&mut beatmap.mapper, value); }
            "Version"       => { data.name = value; }
            "BeatmapID"     => { data.id = value.parse().ok().and_then(|x| NonZeroU32::new(x)); }
            "BeatmapSetID"  => { beatmap.id = value.parse().ok().and_then(|x| NonZeroU32::new(x)); }

            _ => { }
        }
    }

    return Some(data);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = MySqlPool::connect("mysql://root:root@localhost:3306/test").await?;
    let dir_entries = std::fs::read_dir("./beatmapsets/")?;

    for entry in dir_entries {
        let Ok(entry) = entry else { continue };
        let Ok(entry_type) = entry.file_type() else { continue };
        if entry_type.is_dir() { continue };

        if let Some(filename) = entry.file_name().to_str() {
            if !filename.ends_with(".osz") { continue };
        } else { continue };
        
        let mut file = File::open(entry.path()).await?;
        let mut zip = ZipFileReader::with_tokio(&mut file).await?;

        let mut beatmap = Beatmap::default();
        for i in 0 .. zip.file().entries().len() {
            let mut reader = zip.reader_with_entry(i).await?;
            let filename = reader.entry().filename().as_str()?.to_owned();

            if filename.ends_with(".osu") {
                let mut diff = String::new();
                reader.read_to_string_checked(&mut diff).await?;
                let Some(meta) = extract_metadata(&diff, &mut beatmap) else { continue };
                let Some(filename) = meta.id.map(|x| format!("./beatmaps/{}.osu", x.get())) else { continue };
                fs::write(filename, diff.as_bytes()).await?;
            }
        }

        println!("{:#?}", &beatmap);
        sqlx::query("INSERT INTO `test`.`beatmap` (`id`, `artist`, `artist_unicode`, `title`, `title_unicode`, `mapper`) VALUES (?, ?, ?, ?, ?, ?);")
            .bind(beatmap.id.map(|x| x.get()).unwrap_or(0))
            .bind(beatmap.artist)
            .bind(beatmap.artist_unicode)
            .bind(beatmap.title)
            .bind(beatmap.title_unicode)
            .bind(beatmap.mapper)
            .execute(&pool)
            .await?;
    }

    return Ok(());
}
