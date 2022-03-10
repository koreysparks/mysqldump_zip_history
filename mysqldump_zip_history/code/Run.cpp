#include<unistd.h>
#include<sys/types.h>
#include<sys/wait.h>

#include "db.hpp"
using namespace pp;


CLog	g_log("data_backup", false, true);



int main(int argc, char* argv[])
{
	string ip = argv[1];
	int port = 3306;
	string user = argv[2];
	string pwd = argv[3];
	string dbName = argv[4];
	string destDir = argv[5];
	string time = CLog::getTimeStampS();

	assert(!ip.empty() && !user.empty() && !pwd.empty() && !dbName.empty() );
	CDB db(ip, port, dbName, user, pwd, &g_log);
	vector<string> tables;
	db.query("show tables;");

	// 查询到的数据，只有一个表名字段
	vector<map<string, string> > tablesFileds = db.getData();

	for(size_t p = 0; p < tablesFileds.size(); ++p)
	{
		map<string, string> oneRow = tablesFileds[p];
		for(map<string, string>::const_iterator iter = oneRow.begin(); iter != oneRow.end(); ++iter)
		{
			tables.push_back(iter->second);
		}
	}

	string dumpOrderStr = "";
	string dumpBkStr= "";
	for(size_t i = 0; i < tables.size(); ++i)
	{
		if(string::npos != tables[i].find("t_fut_"))
		{
			dumpOrderStr += tables[i];
			dumpOrderStr += " ";
		}
		else if(string::npos != tables[i].find("t_bk_data_"))
		{
			dumpBkStr += tables[i];
			dumpBkStr += " ";
		}
		else
		{}
	}

	if(!dumpOrderStr.empty())
	{
		string sqlName = dbName + "_order_" + time + ".sql";
		string dump = "mysqldump -h" + ip + " -u" + user + " -p" + pwd + " " + dbName + " --lock-tables " + dumpOrderStr + " >";
		string zipName = destDir + "_order_dump_" + time + ".zip";
		system((dump + sqlName).c_str());
		system(("zip " + zipName + " " + sqlName).c_str());
		system(("rm " + sqlName).c_str());
		printf("dump order data finish!\n");
	}

	if(!dumpBkStr.empty())
	{
		string sqlName = dbName + "_bk_" + time + ".sql";
		string dump = "mysqldump -h" + ip + " -u" + user + " -p" + pwd + " " + dbName + " --lock-tables " + dumpBkStr + " >" ;
		string zipName = destDir + "_bk_dump_" + time + ".zip";
		system((dump + sqlName).c_str());
		system(("zip " + zipName + " " + sqlName).c_str());
		system(("rm " + sqlName).c_str());
		printf("dump bk data finish!\n");
	}

	return 0;
}









