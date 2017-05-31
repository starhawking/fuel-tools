#!/usr/bin/env python

import psycopg2
import json
import logging


logging.basicConfig(level=logging.WARN)

def get_conn(database, **conn_kwargs):
	def internal_decorator(f):
		def wrapper(*args,**kwargs):
			logging.debug("database\t{}".format(database))
			try:
				if 'conn' in conn_kwargs:
					conn = conn_kwargs['conn']
					dsn = conn.get_dsn_parameters()
				else:
					conn = psycopg2.connect(dbname=database)
				
			except:
				logging.exception("Failed to connect to Database:\t{}".format(database))
			logging.debug("Database\t{}connected\t{}".format(database, not conn.closed))
			ret_val = f(conn, *args, **kwargs)
			return ret_val
		return wrapper
	return internal_decorator


def get_cursor(**cur_kwargs):
	def internal_decorator(f):
		def wrapper(conn, *wargs,**wkwargs):
			cur= conn.cursor()
			ret_val = f(cur, *wargs, **wkwargs)
			conn.commit()
			return ret_val
		return wrapper
	return internal_decorator

@get_conn('nailgun')
@get_cursor()
def nailgun_attribs(cur):
	cur.execute("""select * from attributes""")
	fetched = cur.fetchall()
	cols = ('id','cluster_id','editable','generated')
	identity = lambda x: x
	col_funcs = (identity,identity,json.loads, json.loads)
	func_pairs = [zip(col_funcs, x) for x in fetched]
	cleaned = [map(lambda x: x[0](x[1]), x) for x in func_pairs]
	return cleaned 

@get_conn('nailgun')
@get_cursor()
def update_env_email(cur, env_attribs, env_id, new_email):
	assert(type(env_id) == int)
	assert(type(new_email) == str)
	matched_attrib = [x for x in env_attribs if x[0] == env_id]
	if len(matched_attrib) != 1:
		logging.exception("Unable to match Env ID: {}".format(env_id))
		raise SystemExit	
	env_attrib = matched_attrib[0]
	old_email = env_attrib[2]['access']['email']['value']
	env_attrib[2]['access']['email']['value']=new_email
	new_json = json.dumps(env_attrib[2])
	query_repr = cur.execute('UPDATE attributes SET editable =%(json)s WHERE id = %(id)s;', {'json': new_json, 'id': env_id})
	#query_repr = cur.mogrify('UPDATE attributes SET editable =%(json)s WHERE id = %(id)s;', {'json': new_json, 'id': env_id})
	return query_repr

def test_main():
	nga = nailgun_attribs()
	print [(x[0], x[2]['access']['email']['value']) for x in nga]
	new_email='user@localhost'
	env_id=1
	uga = update_env_email(nga[0], env_id=env_id, new_email=new_email)

	#nga = nailgun_attribs()
	#print [(x[0], x[2]['access']['email']['value']) for x in nga]


if __name__ == '__main__':
	import os
	import sys
	import argparse
	parser = argparse.ArgumentParser(description='Update the admin email in Fuel')
	parser.add_argument('--email', type=str, help='New Email Address', required=True)
	parser.add_argument('--env', type=int , help='Environment ID to Update', required=True)
	parser.add_argument('--force', help='Do not verify before updating', action='store_true')
	args = parser.parse_args()
	print "Updating Env: {} with Email {}".format(args.env, args.email)
	if args.force != True:
		cont = raw_input("Type Yes to continue:\n")
		if cont.lower() != 'yes':
			print "\nNot Updating"
			raise SystemExit
	update_env_email(nailgun_attribs(), env_id=args.env, new_email=args.email)
	print "Env {} Updated!".format(args.env)
