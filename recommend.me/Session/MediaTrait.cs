namespace recommend.me.Session
{
	public class MediaTrait
	{
		// The string prompted to the user
		public required string Name { get; init; }

		// The internal trait name, id, or keyword used with the database
		public required string Id { get; init; }

		// A basic description of what this trait is
		public required string Description { get; init; }

		// URL to a page for more information
		public required string Url { get; init; }
	}
}
