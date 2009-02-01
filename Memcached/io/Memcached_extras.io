Memcached do(

	with := method(servers,
		m := self clone
		list(servers) flatten foreach(s, m addServer(s))
		m
	)

	at := method(key,
		e := try(
			value := get(key)
		) 

		e catch(Exception,
			if(e error == "NOT FOUND", value := nil, e pass)		
		)

		value
	)

	atPut := method(key, value,
		set(key, value)
		self
	)

	removeAt := method(key,
		delete(key)
		self
	)

)
