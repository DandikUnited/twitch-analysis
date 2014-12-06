package uk.ac.qmul.bigdata.TwitchAnalysis;


import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

    public class StreamLengthPartitioner extends Partitioner<Text, LongWritable> {
 
        public int getPartition(Text key, LongWritable value, int numReduceTasks) {
 
            String [] bigKey = key.toString().split("\t");
            String gameName = bigKey[0];
                       
            //this is done to avoid performing mod with 0
            if(numReduceTasks == 0)
                return 0;
 
            if (gameName == "leagueoflegends"){
            	return 0 % numReduceTasks;
            	}
            	if (gameName == "minecraft"){
            	return 1 % numReduceTasks;
            	}
            	if (gameName == "worldofwarcraftmistsofpandaria"){
            	return 2 % numReduceTasks;
            	}
            	if (gameName == "counterstrikeglobaloffensive"){
            	return 3 % numReduceTasks;
            	}
            	if (gameName == "callofdutyghosts"){
            	return 4 % numReduceTasks;
            	}
            	if (gameName == "dota2"){
            	return 5 % numReduceTasks;
            	}
            	if (gameName == "battlefield4"){
            	return 6 % numReduceTasks;
            	}
            	if (gameName == "fifa14"){
            	return 7 % numReduceTasks;
            	}
            	if (gameName == "hearthstoneheroesofwarcraft"){
            	return 8 % numReduceTasks;
            	}
            	if (gameName == "diabloiiireaperofsouls"){
            	return 9 % numReduceTasks;
            	}
            	if (gameName == "callofdutyblackopsii"){
            	return 10 % numReduceTasks;
            	}
            	if (gameName == "dayz"){
            	return 11 % numReduceTasks;
            	}
            	if (gameName == "titanfall"){
            	return 12 % numReduceTasks;
            	}
            	if (gameName == "smite"){
            	return 13 % numReduceTasks;
            	}
            	if (gameName == "watchdogsa"){
            	return 14 % numReduceTasks;
            	}
            	if (gameName == "starcraftiiheartoftheswarm"){
            	return 15 % numReduceTasks;
            	}
            	if (gameName == "darksoulsii"){
            	return 16 % numReduceTasks;
            	}
            	if (gameName == "runescape"){
            	return 17 % numReduceTasks;
            	}
            	if (gameName == "nba2k14"){
            	return 18 % numReduceTasks;
            	}
            	if (gameName == "worldoftanks"){
            	return 19 % numReduceTasks;
            	}
            	if (gameName == "armaiii"){
            	return 20 % numReduceTasks;
            	}
            	if (gameName == "diabloiii"){
            	return 21 % numReduceTasks;
            	}
            	if (gameName == "destiny"){
            	return 22 % numReduceTasks;
            	}
            	if (gameName == "wildstar"){
            	return 23 % numReduceTasks;
            	}
            	if (gameName == "pathofexile"){
            	return 24 % numReduceTasks;
            	}
            	if (gameName == "outlast"){
            	return 25 % numReduceTasks;
            	}
            	if (gameName == "thelastofusaremastered"){
            	return 26 % numReduceTasks;
            	}
            	if (gameName == "finalfantasyxivonlinearealmreborn"){
            	return 27 % numReduceTasks;
            	}
            	if (gameName == "osu"){
            	return 28 % numReduceTasks;
            	}
            	if (gameName == "theelderscrollsonline"){
            	return 29 % numReduceTasks;
            	}
            	if (gameName == "grandtheftautov"){
            	return 30 % numReduceTasks;
            	}
            	if (gameName == "warframe"){
            	return 31 % numReduceTasks;
            	}
            	if (gameName == "destinybeta"){
            	return 32 % numReduceTasks;
            	}
            	if (gameName == "maddennfl25"){
            	return 33 % numReduceTasks;
            	}
            	if (gameName == "assassinscreedivblackflag"){
            	return 34 % numReduceTasks;
            	}
            	if (gameName == "darksouls"){
            	return 35 % numReduceTasks;
            	}
            	if (gameName == "finalfantasyxivarealmreborn"){
            	return 36 % numReduceTasks;
            	}
            	if (gameName == "guildwars2"){
            	return 37 % numReduceTasks;
            	}
            	if (gameName == "teamfortress2"){
            	return 38 % numReduceTasks;
            	}
            	if (gameName == "archeage"){
            	return 39 % numReduceTasks;
            	}
            	if (gameName == "rust"){
            	return 40 % numReduceTasks;
            	}
            	if (gameName == "worldofwarcraft"){
            	return 41 % numReduceTasks;
            	}
            	if (gameName == "theelderscrollsvskyrim"){
            	return 42 % numReduceTasks;
            	}
            	if (gameName == "infestationsurvivorstories"){
            	return 43 % numReduceTasks;
            	}
            	if (gameName == "garrysmod"){
            	return 44 % numReduceTasks;
            	}
            	if (gameName == "watchdogs"){
            	return 45 % numReduceTasks;
            	}
            	if (gameName == "infamoussecondsona"){
            	return 46 % numReduceTasks;
            	}
            	if (gameName == "borderlands2"){
            	return 47 % numReduceTasks;
            	}
            	if (gameName == "heroesofthestorm"){
            	return 48 % numReduceTasks;
            	}
            	if (gameName == "callofdutyghostsgoldedition"){
            	return 49 % numReduceTasks;
            	}
            	if (gameName == "warthunder"){
            	return 50 % numReduceTasks;
            	}
            	if (gameName == "callofdutyaghosts"){
            	return 51 % numReduceTasks;
            	}
            	if (gameName == "payday2"){
            	return 52 % numReduceTasks;
            	}
            	if (gameName == "dcuniverseonline"){
            	return 53 % numReduceTasks;
            	}
            	if (gameName == "forzamotorsport5"){
            	return 54 % numReduceTasks;
            	}
            	if (gameName == "maddennfl15"){
            	return 55 % numReduceTasks;
            	}
            	if (gameName == "tera"){
            	return 56 % numReduceTasks;
            	}
            	if (gameName == "starwarstheoldrepublic"){
            	return 57 % numReduceTasks;
            	}
            	if (gameName == "thebindingofisaac"){
            	return 58 % numReduceTasks;
            	}
            	if (gameName == "pokamonxy"){
            	return 59 % numReduceTasks;
            	}
            	if (gameName == "planetside2"){
            	return 60 % numReduceTasks;
            	}
            	if (gameName == "trialsfusion"){
            	return 61 % numReduceTasks;
            	}
            	if (gameName == "heroesofnewerth"){
            	return 62 % numReduceTasks;
            	}
            	if (gameName == "plantsvszombiesgardenwarfare"){
            	return 63 % numReduceTasks;
            	}
            	if (gameName == "apbreloaded"){
            	return 64 % numReduceTasks;
            	}
            	if (gameName == "eurotrucksimulator2"){
            	return 65 % numReduceTasks;
            	}
            	if (gameName == "halo3"){
            	return 66 % numReduceTasks;
            	}
            	if (gameName == "southparkthestickoftruth"){
            	return 67 % numReduceTasks;
            	}
            	if (gameName == "easportsaufca"){
            	return 68 % numReduceTasks;
            	}
            	if (gameName == "metalgearsolidvgroundzeroes"){
            	return 69 % numReduceTasks;
            	}
            	if (gameName == "thief"){
            	return 70 % numReduceTasks;
            	}
            	if (gameName == "unturned"){
            	return 71 % numReduceTasks;
            	}
            	if (gameName == "left4dead2"){
            	return 72 % numReduceTasks;
            	}
            	if (gameName == "infamousasecondson"){
            	return 73 % numReduceTasks;
            	}
            	if (gameName == "clashofclans"){
            	return 74 % numReduceTasks;
            	}
            	if (gameName == "supermario64"){
            	return 75 % numReduceTasks;
            	}
            	if (gameName == "magicthegathering"){
            	return 76 % numReduceTasks;
            	}
            	if (gameName == "mlba14theshowa"){
            	return 77 % numReduceTasks;
            	}
            	if (gameName == "dawngate"){
            	return 78 % numReduceTasks;
            	}
            	if (gameName == "kerbalspaceprogram"){
            	return 79 % numReduceTasks;
            	}
            	if (gameName == "thelegendofzeldaocarinaoftime"){
            	return 80 % numReduceTasks;
            	}
            	if (gameName == "deadrising3"){
            	return 81 % numReduceTasks;
            	}
            	if (gameName == "pt"){
            	return 82 % numReduceTasks;
            	}
            	if (gameName == "battlefield3"){
            	return 83 % numReduceTasks;
            	}
            	if (gameName == "diabloiiireaperofsoulsaultimateeviledition"){
            	return 84 % numReduceTasks;
            	}
            	if (gameName == "needforspeedrivals"){
            	return 85 % numReduceTasks;
            	}
            	if (gameName == "deadnationaapocalypseedition"){
            	return 86 % numReduceTasks;
            	}
            	if (gameName == "destinyfirstlookalpha"){
            	return 87 % numReduceTasks;
            	}
            	if (gameName == "mygreatgame"){
            	return 88 % numReduceTasks;
            	}
            	if (gameName == "mariokart8"){
            	return 89 % numReduceTasks;
            	}
            	if (gameName == "terraria"){
            	return 90 % numReduceTasks;
            	}
            	if (gameName == "wolfensteintheneworder"){
            	return 91 % numReduceTasks;
            	}
            	if (gameName == "eveonline"){
            	return 92 % numReduceTasks;
            	}
            	if (gameName == "tombraiderdefinitiveedition"){
            	return 93 % numReduceTasks;
            	}
            	if (gameName == "killzoneshadowfall"){
            	return 94 % numReduceTasks;
            	}
            	if (gameName == "diabloiiireaperofsoulsaultimateevileditionenglish"){
            	return 95 % numReduceTasks;
            	}
            	if (gameName == "dontstarveconsoleedition"){
            	return 96 % numReduceTasks;
            	}
            	if (gameName == "companyofheroes2"){
            	return 97 % numReduceTasks;
            	}
            	if (gameName == "sniperelite3"){
            	return 98 % numReduceTasks;
            	}
            	if (gameName == "killerinstinct"){
            	return 99 % numReduceTasks;
            	}
            	else return 31;

        }

		public void configure(JobConf arg0) {
			// TODO Auto-generated method stub
			
		}

    }
 

