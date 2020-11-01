const { MongoClient } = require("mongodb");
var path = require("path");
fs = require("fs");

let script = async (client) => {
  contents = fs.readFileSync(
    path.resolve(__dirname, "./dbdump/movies.dsv"),
    "utf8"
  );
  sh = contents.split("\n");

  let bad = [];
  let good = [];

  let document = {
    budget: 0,
    genres: [], //1
    homepage: "",
    id: null,
    keywords: null,
    original_language: 0,
    original_title: "",
    overview: "",
    popularity: 0,
    production_companies: [], //9
    production_countries: [], //10
    release_date: 0,
    revenue: "",
    runtime: 0,
    spoken_languages: [], //14
    status: "",
    tagline: "",
    title: "",
    vote_average: 0,
    vote_count: 0,
  };

  sh.forEach(async (row, index) => {
    
    if(index == 0)
      return;
    let columns = row.split("|");
    if (columns.length != 20) bad.push(index);
    let errorOcurred = false;

    document = {
      budget: 0,
      genres: [], //1
      homepage: "",
      id: null,
      keywords: null,
      original_language: 0,
      original_title: "",
      overview: "",
      popularity: 0,
      production_companies: [], //9
      production_countries: [], //10
      release_date: 0,
      revenue: "",
      runtime: 0,
      spoken_languages: [], //14
      status: "",
      tagline: "",
      title: "",
      vote_average: 0,
      vote_count: 0,
    };

    columns.forEach((col, idx) => {
      let parsedCol = null;
      try {
        if (
          index != 0 &&
          !bad.includes(index) &&
          [1, 4, 9, 10, 14].includes(idx)
        )
          parsedCol = JSON.parse(
            col.substring(1, col.length - 1).replace(/\"\"/gi, '"')
          );
      } catch (err) {
        if (col == []) {
          parsedCol = [];
          console.log("dejar pasar: ", index, col);
        }
        errorOcurred = true;
        return;
      }

      try {
        switch (idx) {
          case 0:
            document.budget = parseInt(col);
            break;
          case 1:
            document.genres = parsedCol;
            break;
          case 2:
            document.homepage = col;
            break;
          case 3:
            document.id = col;
            break;
          case 4:
            document.keywords = parsedCol;
            break;
          case 5:
            document.original_language = col;
            break;
          case 6:
            document.original_title = col;
            break;
          case 7:
            document.overview = col;
            break;
          case 8:
            document.popularity = parseFloat(col);
            break;
          case 9:
            document.production_companies = parsedCol;
            break;
          case 10:
            document.production_countries = parsedCol;
            break;
          case 11:
            document.release_date = col;
          case 12:
            document.revenue = parseFloat(col);
            break;
          case 13:
            document.runtime = parseInt(col);
            break;
          case 14:
            document.spoken_languages = parsedCol;
            break; //14
          case 15:
            document.status = col;
            break;
          case 16:
            document.tagline = col;
            break;
          case 17:
            document.title = col;
          case 18:
            document.vote_average = parseFloat(col);
            break;
          case 19:
            document.vote_count = parseFloat(col);
            break;
        }
      } catch (err) {
        errorOcurred = true;
      }
    });

    if (!errorOcurred) {
      //guardar en base de datos
      good.push(document);
    }
  });

  return good;
};

async function listDatabases(client) {
  databasesList = await client.db().admin().listDatabases();
  console.log("Databases:");
  databasesList.databases.forEach((db) => console.log(` - ${db.name}`));
}

async function main() {
  const uri = "mongodb://0.0.0.0:27017/bd2";
  const client = new MongoClient(uri, { useUnifiedTopology: true });

  try {
    // Connect to the MongoDB cluster
    await client.connect();
    // Make the appropriate DB calls
    await listDatabases(client);
    let data = await script(client);
    client.db().collection("movies").insertMany(data);
  } catch (e) {
    console.error(e);
  }
}

main().catch(console.error);

