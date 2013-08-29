-record(context, {
	connection :: pid(),
	database   :: mongo:database(),
	read_mode  :: mongo_connection:read_mode(),
	write_mode  :: mongo_connection:write_mode()
}).
