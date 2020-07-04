#include <sdb.h>
#include <assert.h>

int main(int argc, char **argv) {
	int rc = 0;
	SdbCas r;
	SdbCas cas = 1;

	Sdb *s = sdb_new (NULL, NULL, 0);
	assert (s != NULL);
	r = cas;
	assert (sdb_set (s, "hello", "world", &cas) == true);
	r = cas;
	sdb_const_get (s, "hello", &cas);
	printf ("[test] r%zu = c%zu\n", r, cas);
	if (r != cas) {
		printf ("error\n");
		rc = 1;
	} else {
		printf ("  ok\n");
	}

	if (!sdb_set (s, "hello", "world", &cas)) {
		eprintf ("Cannot set\n");
	}
	sdb_const_get (s, "hello", &cas);
	printf ("[test] r%zu = c%zu\n", r, cas);
	if (r != cas) {
		printf ("error\n");
		rc = 1;
	} else {
		printf ("  ok\n");
	}
	printf ("[test] r%zu = c%zu\n", r, cas);
	if (r == 0) {
		printf ("error\n");
		rc = 1;
	} else {
		printf ("  ok\n");
	}
	sdb_free (s);
	return rc;
}
