//write logic would be here: 
	//if coordinator : hash -> send to other nodes , await reply , if crash detected->repair
	//if not coordinator : wirte to commitlog->memtable->check if memtable>capacity ->SStable