import ConfigParser

config = ConfigParser.RawConfigParser()
config.add_section('Database')
config.set('Database', 'dbname', 'tpch')
config.set('Database', 'host', 'localhost')
config.set('Database', 'username', 'root')

config.add_section('TinyCube server')
config.set('TinyCube server', 'host', 'localhost')
config.set('TinyCube server', 'port', '5000')

config.add_section('TinyCube client')
config.set('TinyCube client', 'host', 'localhost')
config.set('TinyCube client', 'port', '5000')

with open('TinyCube.cfg', 'wb') as configfile:
	    config.write(configfile)
