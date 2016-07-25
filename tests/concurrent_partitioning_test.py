'''
 concurrent_partitioning_test.py
  		Tests concurrent partitioning worker with simultaneous update queries

 Copyright (c) 2015-2016, Postgres Professional
'''

import psycopg2
import argparse
import getpass
import time

class PasswordPromptAction(argparse.Action):
    def __call__(self, parser, args, values, option_string=None):
        password = getpass.getpass()
        setattr(args, self.dest, password)

class SetupException(Exception): pass
class TeardownException(Exception): pass

setup_cmd = [
	'drop extension if exists pg_pathman cascade',
	'drop table if exists abc cascade',
	'create extension pg_pathman',
	'create table abc(id serial, t text)',
	'insert into abc select generate_series(1, 300000)',
	'select create_hash_partitions(\'abc\', \'id\', 3, p_partition_data := false)',
]

teardown_cmd = [
	'drop table abc cascade',
	'drop extension pg_pathman',
]

def setup(con):
	''' Creates pg_pathman extension, creates table, fills it with data,
		creates partitions
	'''
	print 'setting up...'
	try:
		cur = con.cursor()
		for cmd in setup_cmd:
			cur.execute(cmd)
		con.commit()
		cur.close()
	except Exception, e:
		raise SetupException('Setup failed: %s' % e)
	print 'done!'

def teardown(con):
	''' Drops table and extension '''
	print 'tearing down...'
	try:
		cur = con.cursor()
		for cmd in teardown_cmd:
			cur.execute(cmd)
		con.commit()
		cur.close()
	except Exception, e:
		raise TeardownException('Teardown failed: %s' % e)
	print 'done!'

def main(config):
	''' Main test function

		We create a table, insert a million rows into it, create three hash
		partitions and run concurrent worker to migrate data to partitions.
		Simultaneously we trying to update data to test for deadlocks
	'''

	con = psycopg2.connect(**config)
	setup(con)

	# start concurrent partitioning worker
	cur = con.cursor()

	print 'starting worker..'
	cur.execute('select partition_data_worker(\'abc\')')
	# flag = true

	print 'wait until it finishes..'
	while True:
		# update some rows to check for deadlocks
		cur.execute('''
			update abc set t = 'test'
			where id in (select (random() * 300000)::int from generate_series(1, 3000))
			''')

		cur.execute('select count(*) from pathman_active_workers')
		row = cur.fetchone()
		con.commit()

		# if there is no active workers then it means work is done
		if row[0] == 0:
			break
		time.sleep(1)

	cur.execute('select count(*) from only abc')
	row = cur.fetchone()
	assert(row[0] == 0)
	cur.execute('select count(*) from abc')
	row = cur.fetchone()
	assert(row[0] == 300000)

	teardown(con)
	con.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Concurrent partitioning test')
    parser.add_argument('--host', default='localhost', help='postgres server host')
    parser.add_argument('--port', type=int, default=5432, help='postgres server port')
    parser.add_argument('--user', dest='user', default='postgres', help='user name')
    parser.add_argument('--database', dest='database', default='postgres', help='database name')
    parser.add_argument('--password', dest='password', nargs=0, action=PasswordPromptAction, default='')
    args = parser.parse_args()
    main(args.__dict__)
