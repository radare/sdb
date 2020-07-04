namespace SDB {
	[Compact]
	[CCode (cheader_filename="sdb.h", cname="Sdb", cprefix="sdb_", free_function="sdb_free")]
	public class Sdb {
		/* lifecycle */
		public Sdb (string? path=null, string? file=null, bool locked=false);
		public Sdb ns(string path, bool create);
		public Sdb ns_path(string path, bool create);
		public bool sync ();
		/* query */
		public bool query(string cmd);
		public string querys(string? buf=null, int len=0, string cmd="");
		/* string */
		public bool exists (string key);
		public string @get (string key, out uint32? cas = null);
		public bool @add (string key, string val, out uint32 cas = null);
		public bool @set (string key, string val, out uint32 cas = null);
		/* boolean */
		public bool bool_get (string key, out uint32? cas = null);
		public bool bool_set (string key, bool v, uint32 cas = 0);
		/* arrays */
		public int array_length (string key);
		public string array_get (string key, int idx, out uint32? cas = null);
		public bool array_set (string key, int idx, string val, ref uint32 cas);
		public bool array_set_num (string key, int idx, uint64 val, ref uint32 cas);
		public bool array_delete (string key, int idx, ref uint32 cas);
		public bool array_remove (string key, string val, ref uint32 cas);
		public bool array_remove_num (string key, uint64 val, ref uint32 cas);
		public bool array_contains (string key, string val, out uint32 cas = null);
		/* numeric */
		public bool num_exists (string key);
		public bool num_set (string key, uint64 num, out uint32 cas = null);
		public uint64 num_get (string key, uint32 *cas = null);
		public uint64 num_inc (string key, uint64 n, out uint32 cas = null);
		public uint64 num_dec (string key, uint64 n, uint32 cas = 0);
		/* json */
		public string json_get (string key, string path, out uint32? cas = null);
		public bool json_set (string key, string path, string val, uint32 cas);
//TODO : st64? ut64 here too?
		public int json_num_get (string key, string path, uint32 *cas = null);
		public int json_num_set (string key, string path, int v, uint32 cas = 0);
		public int json_num_inc (string key, string path, int n, uint32 cas = 0);
		public int json_num_dec (string key, string path, int n, uint32 cas = 0);
		public static string json_indent (string json);
		public static string json_unindent (string json);
		/* remove */
		public bool unset (string key, int cas=0);
		public void reset ();
		public void unlink ();
		/* time */
		public uint64 expire_get (string key, out uint32? cas = null);
		public bool expire_set (string key, uint64 time, out uint32 cas = null);
		public static uint64 now ();
	}
}
