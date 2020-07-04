#include <sdb.h>

int main() {
	const char *cp;
	char *p;

	Sdb *s = sdb_new (NULL, NULL, 0);
	sdb_set (s, "array", "111,222,333", 0);
	sdb_array_delete (s, "array", 1, 0);
	cp = sdb_const_get (s, "array", 0);
	printf ("--> %s\n", cp);
	printf ("--> %zd\n", sdb_array_length (s, "array"));
	sdb_array_push (s, "array", "end", 0);
	sdb_array_push (s, "array", "end2", 0);
	cp = sdb_const_get (s, "array", 0);
	printf ("--> %s\n", cp);
	printf ("--> %zd\n", sdb_array_length (s, "array"));
	sdb_array_push (s, "array", "end2", 0);

	printf ("--> POP <--\n");
	p = sdb_array_pop (s, "array", 0);
	if (p) {
		printf ("POP %s\n", p);
		free (p);
	}

	printf ("POP REST %s\n", sdb_const_get (s, "array", 0));

	p = sdb_array_pop (s, "array", 0);
	if (p) {
		printf ("POP %s\n", p);
		free (p);
	}

	printf ("POP REST %s\n", sdb_const_get (s, "array", 0));

	p = sdb_array_pop (s, "array", 0);
	if (p) {
		printf ("POP %s\n", p);
		free (p);
	}

	printf ("POP REST %s\n", sdb_const_get (s, "array", 0));
	sdb_free (s);
	return 0;
}
