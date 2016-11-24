/* sdb - MIT - Copyright 2012-2016 - pancake */

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include "sdb.h"

SDB_API const char *sdb_lock_file(const char *f) {
	static char buf[128];
	size_t len;
	if (!f || !*f) {
		return NULL;
	}
	len = strlen (f);
	if (len + 10 > sizeof buf) {
		return NULL;
	}
	memcpy (buf, f, len);
	strcpy (buf + len, ".lock");
	return buf;
}

#define os_getpid() getpid()

SDB_API bool sdb_lock(const char *s) {
	int fd;
	char *pid, pidstr[64];
	if (!s) {
		return false;
	}
	fd = open (s, O_CREAT | O_TRUNC | O_WRONLY | O_EXCL, SDB_MODE);
	if (fd == -1) {
		return false;
	}
	pid = sdb_itoa (getpid(), pidstr, 10);
	if (pid) {
		if ((write (fd, pid, strlen (pid)) < 0)
			|| (write (fd, "\n", 1) < 0)) {
			close (fd);
			return false;
		}
	}
	close (fd);
	return true;
}

SDB_API int sdb_lock_wait(const char *s) {
	// TODO use flock() here
	// wait forever here?
 	while (!sdb_lock (s)) {
		// if an error has ocurred, is better to abort this function
		// without handing the lock over
		if (errno)
			return 0;

		// TODO: if waiting too much return 0
#if __SDB_WINDOWS__
	 	Sleep (500); // hack
#else
	// TODO use lockf() here .. flock is not much useful (fd, LOCK_EX);
	 	sleep (1); // hack
#endif
 	}
	return 1;
}

SDB_API void sdb_unlock(const char *s) {
	//flock (fd, LOCK_UN);
	if (s && unlink (s) < 0) {
		char buf[256];
		strerror_r(errno, buf, sizeof(buf));
		eprintf ("sdb_unlock: unlink(%s): %s\n", s, buf);
		if (errno != ENOENT && errno != EISDIR) {
			// other error type means that the lockfile could not
			// be removed, so if anyone tries to fetch this lock a
			// deadlock will happen -- abortion is not an option
			// (at least without changing the API).
			eprintf ("sdb_unlock: unlink(%s): aborting for avoiding dead-locks.\n", s);
			exit (1);
		}
	}
}

#if TEST
main () {
	int r;
	r = sdb_lock (".lock");
	printf ("%d\n", r);
	r = sdb_lock (".lock");
	printf ("%d\n", r);
	sdb_unlock (".lock");
	r = sdb_lock (".lock");
	printf ("%d\n", r);
}
#endif
