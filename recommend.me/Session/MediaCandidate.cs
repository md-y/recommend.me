namespace recommend.me.Session
{
	public class MediaCandidate
	{
		// Name of this book, movie, etc.
		public required string Name { get; init; }

		// Id of this piece of media in the database
		public required string Id { get; init; }

		// Url to this media
		public required string Url { get; init; }
	}
}
