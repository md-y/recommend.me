using Microsoft.VisualBasic.FileIO;
using Npgsql;
using System.Collections.Concurrent;
using System.Text;
using System.Text.RegularExpressions;
using System.Web;

string SplitCamelCase(string source)
{
	return String.Join(' ', Regex.Split(source, @"(?<!^)(?=[A-Z])"));
}

string BuildURL(string format, string value)
{
	return String.Format(format, HttpUtility.UrlEncode(value));
}

string ToBase64(string value)
{
	return Convert.ToBase64String(Encoding.UTF8.GetBytes(value));
}

List<Trait> processTropesFile(string path)
{
	var traits = new List<Trait>();
	using (TextFieldParser csvParser = new TextFieldParser(path))
	{
		csvParser.CommentTokens = new string[] { "#" };
		csvParser.SetDelimiters(new string[] { "," });
		csvParser.HasFieldsEnclosedInQuotes = true;

		csvParser.ReadFields();

		while (!csvParser.EndOfData)
		{
			var fields = csvParser.ReadFields();
			if (fields == null || fields.Length < 3) continue;
			traits.Add(new Trait()
			{
				id = fields[1],
				name = SplitCamelCase(fields[2]),
				description = ToBase64(fields[3]),
				url = BuildURL("https://tvtropes.org/pmwiki/pmwiki.php/Main/{0}", fields[2])
			});
		}
	}
	return traits;
}

List<Media> processMediaFile(string path)
{
	var mediaDict = new Dictionary<string, Media>();
	using (TextFieldParser csvParser = new TextFieldParser(path))
	{
		csvParser.CommentTokens = new string[] { "#" };
		csvParser.SetDelimiters(new string[] { "," });
		csvParser.HasFieldsEnclosedInQuotes = true;

		csvParser.ReadFields();

		while (!csvParser.EndOfData)
		{
			var fields = csvParser.ReadFields();
			if (fields == null || fields.Length < 5) continue;

			string title = fields[1];
			string tropeId = fields[4];
			string mediaId = fields[5];

			if (!mediaDict.ContainsKey(mediaId))
			{
				mediaDict[mediaId] = new Media()
				{
					id = mediaId,
					name = SplitCamelCase(title),
					traitIds = new HashSet<string> { tropeId },
					url = BuildURL("https://tvtropes.org/pmwiki/pmwiki.php/Series/{0}", title)
				};
			}
			else
			{
				mediaDict[mediaId].traitIds.Add(tropeId);
			}
		}
	}
	return mediaDict.Values.ToList();
}

async Task InitTables(NpgsqlDataSource db)
{
	var conn = db.OpenConnection();
	await using var batch = new NpgsqlBatch(conn)
	{
		BatchCommands =
		{
			new ("DROP TABLE IF EXISTS media_traits"),
			new ("DROP TABLE IF EXISTS traits"),
			new ("DROP TABLE IF EXISTS media"),
			new ("CREATE TABLE traits( id character varying(16) primary key, name TEXT NOT NULL, description TEXT NOT NULL, url TEXT NOT NULL )"),
			new ("CREATE TABLE media( id character varying(16) primary key, name TEXT NOT NULL, url TEXT NOT NULL )"),
			new ("CREATE TABLE media_traits( media_id character varying(16) REFERENCES media(id), trait_id character varying(16) REFERENCES traits(id), CONSTRAINT media_traits_pk PRIMARY KEY(media_id, trait_id) )")
		}
	};
	await batch.ExecuteReaderAsync();
	await conn.CloseAsync();
}

async Task CopyToDatabase(NpgsqlDataSource db, IEnumerable<Media> allMedia, IEnumerable<Trait> traits)
{
	var conn = db.OpenConnection();

	using (var writer = conn.BeginBinaryImport("COPY media (id, name, url) FROM STDIN (FORMAT BINARY)"))
	{
		foreach (var media in allMedia)
		{
			await writer.StartRowAsync();
			await writer.WriteAsync(media.id);
			await writer.WriteAsync(media.name);
			await writer.WriteAsync(media.url);
		}

		writer.Complete();
	}

	using (var writer = conn.BeginBinaryImport("COPY traits (id, name, description, url) FROM STDIN (FORMAT BINARY)"))
	{
		foreach (var trait in traits)
		{
			await writer.StartRowAsync();
			await writer.WriteAsync(trait.id);
			await writer.WriteAsync(trait.name);
			await writer.WriteAsync(trait.description);
			await writer.WriteAsync(trait.url);
		}

		await writer.CompleteAsync();
	}

	using (var writer = conn.BeginBinaryImport("COPY media_traits (media_id, trait_id) FROM STDIN (FORMAT BINARY)"))
	{
		foreach (var media in allMedia)
		{
			foreach (var traitId in media.traitIds)
			{
				await writer.StartRowAsync();
				await writer.WriteAsync(media.id);
				await writer.WriteAsync(traitId);
			}
		}

		await writer.CompleteAsync();
	}

	await conn.CloseAsync();
}

async Task GenerateData()
{
	string[] mediaFiles = { "./Data/film_tropes.csv", "./Data/lit_tropes.csv", "./Data/tv_tropes.csv" };
	string tropesFile = "./Data/tropes.csv";
	string connectionString = "Host=localhost;Username=postgres;Password=admin;Database=recommendme;CommandTimeout=120";

	string currentDir = Directory.GetCurrentDirectory();
	string tempDir = Path.Join(Path.GetTempPath(), "recommendme");
	Directory.CreateDirectory(tempDir);

	var traits = processTropesFile(Path.Combine(currentDir, tropesFile));
	Console.WriteLine("Processed tropes file: {0}", tropesFile);

	var mediaBag = new ConcurrentBag<IEnumerable<Media>>();
	Parallel.ForEach(mediaFiles, file =>
	{
		string path = Path.Combine(currentDir, file);
		Console.WriteLine("Reading \"{0}\" on thread {1}", file, Thread.CurrentThread.ManagedThreadId);
		var mediaList = processMediaFile(path);
		mediaBag.Add(mediaList);
		Console.WriteLine("Finished processing {0}", file);
	});

	IEnumerable<Media> allMedia = mediaBag.Aggregate((curr, next) => curr.Concat(next));

	await using var db = NpgsqlDataSource.Create(connectionString);

	await InitTables(db);
	Console.WriteLine("Reset tables");

	Console.WriteLine("Started copy to datbase");
	await CopyToDatabase(db, allMedia, traits);
	Console.WriteLine("Copied to database");

	Console.WriteLine("Finished!");
}

await GenerateData();

class Trait
{
	public required string id;
	public required string name;
	public required string description;
	public required string url;
}

class Media
{
	public required string id;
	public required string name;
	public required string url;
	public required HashSet<string> traitIds;
}