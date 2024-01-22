using Npgsql;

namespace recommend.me.Session
{
	public class QuestionSession
	{
		private Dictionary<Answer, List<MediaTrait>> _traitAnswers = new Dictionary<Answer, List<MediaTrait>>();

		private NpgsqlDataSource _dataSource;

        public QuestionSession() {
            // TODO: Move this string to a config file
            _dataSource = NpgsqlDataSource.Create("Host=localhost;Username=postgres;Password=admin;Database=recommendme");
            foreach (Answer ans in Enum.GetValues(typeof(Answer)))
			{
				_traitAnswers[ans] = new List<MediaTrait>();
			}
		}

		public async Task<MediaCandidate[]> GetCandidates(int limit = 20)
		{
            var conn = _dataSource.OpenConnection();
            await using var batch = new NpgsqlBatch(conn)
            {
                BatchCommands = {
                    new ("DROP TABLE IF EXISTS excluded_traits"),
                    new ("DROP TABLE IF EXISTS required_traits"),
                    new ("CREATE TEMPORARY TABLE excluded_traits ( id character varying(16) PRIMARY KEY )"),
                    new ("CREATE TEMPORARY TABLE required_traits ( id character varying(16) PRIMARY KEY )"),
                }
            };

            if (_traitAnswers[Answer.No].Count > 0)
            {
                batch.BatchCommands.Add(
                    CreateMediaListCommand("INSERT INTO excluded_traits (id) VALUES {0}", _traitAnswers[Answer.No])
                );
            }
            if (_traitAnswers[Answer.Yes].Count > 0)
            {
                batch.BatchCommands.Add(
                    CreateMediaListCommand("INSERT INTO required_traits (id) VALUES {0}", _traitAnswers[Answer.Yes])
                );
            }

            // TODO: Find a way to use SQL comamnds that aren't just big strings
            batch.BatchCommands.Add(new(
                String.Format(
                    "WITH RequiredTraitCount AS ( SELECT count(id) AS required_count FROM required_traits ), FilteredMedia AS ( WITH ExcludedMedia AS ( SELECT DISTINCT media_id AS id FROM media_traits mt INNER JOIN excluded_traits et ON et.id = mt.trait_id ), PossibleMedia AS ( SELECT media_id, count(media_id) as trait_count FROM media_traits mt INNER JOIN required_traits rt ON rt.id = mt.trait_id WHERE NOT EXISTS ( SELECT * FROM ExcludedMedia em WHERE em.id = mt.media_id ) GROUP BY mt.media_id UNION SELECT media_id, 0 as trait_count FROM media_traits, RequiredTraitCount rtc WHERE rtc.required_count = 0 EXCEPT ( SELECT id AS media_id, 0 as trait_count FROM ExcludedMedia ) ) SELECT media_id FROM PossibleMedia, RequiredTraitCount rtc WHERE trait_count >= rtc.required_count ) SELECT id, name, url FROM FilteredMedia fm INNER JOIN media m ON fm.media_id = m.id LIMIT {0};",
                    limit
                )
            ));

            await using var reader = await batch.ExecuteReaderAsync();

            if (!reader.HasRows) return [];

            List<MediaCandidate> candidates = new List<MediaCandidate>();
            while (await reader.ReadAsync())
            {
                candidates.Add(new MediaCandidate()
                {
                    Id = reader.GetString(0),
                    Name = reader.GetString(1),
                    Url = reader.GetString(2),
                });
            }

            return candidates.ToArray();
        }

		public async Task<MediaTrait?> GetNewPromptTrait()
		{
            var conn = _dataSource.OpenConnection();
			await using var batch = new NpgsqlBatch(conn) {
				BatchCommands = {
					new ("DROP TABLE IF EXISTS excluded_traits"),
					new ("DROP TABLE IF EXISTS required_traits"),
					new ("DROP TABLE IF EXISTS ignored_traits"),
					new ("CREATE TEMPORARY TABLE excluded_traits ( id character varying(16) PRIMARY KEY )"),
					new ("CREATE TEMPORARY TABLE required_traits ( id character varying(16) PRIMARY KEY )"),
					new ("CREATE TEMPORARY TABLE ignored_traits ( id character varying(16) PRIMARY KEY )"),
				}	
            };

            if (_traitAnswers[Answer.No].Count > 0)
            {
                batch.BatchCommands.Add(
                    CreateMediaListCommand("INSERT INTO excluded_traits (id) VALUES {0}", _traitAnswers[Answer.No])
                );
            }
            if (_traitAnswers[Answer.Yes].Count > 0)
            {
                batch.BatchCommands.Add(
                    CreateMediaListCommand("INSERT INTO required_traits (id) VALUES {0}", _traitAnswers[Answer.Yes])
                );
				// TODO: Update SQL so we don't need two commands?
                batch.BatchCommands.Add(
                    CreateMediaListCommand("INSERT INTO ignored_traits (id) VALUES {0}", _traitAnswers[Answer.Yes])
                );
            }
            if (_traitAnswers[Answer.DontCare].Count > 0)
            {
                batch.BatchCommands.Add(
                    CreateMediaListCommand("INSERT INTO ignored_traits (id) VALUES {0}", _traitAnswers[Answer.DontCare])
                );
            }

            batch.BatchCommands.Add(
                new("WITH RequiredTraitCount AS ( SELECT count(id) AS required_count FROM required_traits ), FilteredMedia AS ( WITH ExcludedMedia AS ( SELECT DISTINCT media_id AS id FROM media_traits mt INNER JOIN excluded_traits et ON et.id = mt.trait_id ), PossibleMedia AS ( SELECT media_id, count(media_id) as trait_count FROM media_traits mt INNER JOIN required_traits rt ON rt.id = mt.trait_id WHERE NOT EXISTS ( SELECT * FROM ExcludedMedia em WHERE em.id = mt.media_id ) GROUP BY mt.media_id UNION SELECT media_id, 0 as trait_count FROM media_traits, RequiredTraitCount rtc WHERE rtc.required_count = 0 EXCEPT ( SELECT id AS media_id, 0 as trait_count FROM ExcludedMedia ) ) SELECT media_id FROM PossibleMedia, RequiredTraitCount rtc WHERE trait_count >= rtc.required_count ), CountedTraits AS ( SELECT trait_id, count(trait_id) AS trait_count FROM FilteredMedia fm INNER JOIN media_traits mt ON mt.media_id = fm.media_id GROUP BY trait_id ), TopTrait AS ( SELECT trait_id AS id, trait_count FROM CountedTraits EXCEPT ( SELECT it.id, ct.trait_count FROM ignored_traits it, CountedTraits ct ) ORDER BY trait_count DESC LIMIT 1 ) SELECT * FROM TopTrait tt INNER JOIN traits t ON t.id = tt.id")
            );

            await using var reader = await batch.ExecuteReaderAsync();

			if (!reader.HasRows) return null;
			
			await reader.ReadAsync(); // Wait for 1 row

			string description = System.Text.Encoding.UTF8.GetString(Convert.FromBase64String(reader.GetString(4)));

			var trait = new MediaTrait()
			{
				Description = description,
				Id = reader.GetString(0),
				Name = reader.GetString(3),
				Url = reader.GetString(5),
			};

			await conn.CloseAsync();

			return trait;
		}

		public void AnswerPromptTrait(MediaTrait trait, Answer answer)
		{
			_traitAnswers[answer].Add(trait);
		}

		private NpgsqlBatchCommand CreateMediaListCommand(string format, List<MediaTrait> traits)
		{
			var traitsSet = new HashSet<MediaTrait>(traits); // Ensure no duplicates
			var valueStrings = traitsSet.Select(t => "('" + t.Id + "')");
			var valueString = String.Join(',', valueStrings);
			var commandString = String.Format(format, valueString);
			return new NpgsqlBatchCommand(commandString);
        }
	}
}
