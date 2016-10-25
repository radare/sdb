#include "minunit.h"
#include <sdb.h>

bool test_sdb_array_push_pop(void) {
	Sdb *db = sdb_new (NULL, NULL, false);
	char *p;

	sdb_array_push (db, "foo", "foo", 0);
	sdb_array_push (db, "foo", "bar", 0);
	sdb_array_push (db, "foo", "cow", 0);

	mu_assert_streq (sdb_const_get (db, "foo", 0), "cow,bar,foo", "Not all items found");

	p = sdb_array_pop (db, "foo", NULL);
	mu_assert_streq (p, "cow", "cow was not at the top");
	p = sdb_array_pop (db, "foo", NULL);
	mu_assert_streq (p, "bar", "bar was not at the top");
	p = sdb_array_pop (db, "foo", NULL);
	mu_assert_streq (p, "foo", "foo was not at the top");
	p = sdb_array_pop (db, "foo", NULL);
	mu_assert_eq ((int)p, (int)NULL, "there shouldn't be any element in the array");

	sdb_free (db);
	mu_end;
}

bool test_sdb_array_add_remove(void) {
	Sdb *db = sdb_new (NULL, NULL, false);
	sdb_array_add (db, "foo", "foo", 0);
	sdb_array_add (db, "foo", "bar", 0);
	sdb_array_add (db, "foo", "cow", 0);

	mu_assert_streq (sdb_const_get (db, "foo", 0), "foo,bar,cow", "Not all items found");

	sdb_array_remove (db, "foo", "bar", 0);
	mu_assert_streq (sdb_const_get (db, "foo", 0), "foo,cow", "bar was not deleted");
	sdb_array_remove (db, "foo", "nothing", 0);
	mu_assert_streq (sdb_const_get (db, "foo", 0), "foo,cow", "nothing should be deleted");
	sdb_array_remove (db, "foo", "cow", 0);
	sdb_array_remove (db, "foo", "foo", 0);
	mu_assert_eq ((int)sdb_const_get (db, "foo", 0), (int)NULL, "all elements should be deleted");

	sdb_free (db);
	mu_end;
}


int all_tests() {
	mu_run_test (test_sdb_array_push_pop);
	mu_run_test (test_sdb_array_add_remove);
	return tests_passed != tests_run;
}

int main(int argc, char **argv) {
	return all_tests ();
}
