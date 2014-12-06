package uk.ac.qmul.bigdata.TwitchAnalysis;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Date;
import java.text.Normalizer;
import java.text.SimpleDateFormat;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StreamLengthMapper extends
Mapper<Object, TwitchDataRecord, Text, LongWritable> {

	public void map(Object key, TwitchDataRecord value, Context context)
			throws IOException, InterruptedException {
		Text filteredIntermediateResult = new Text();
		String filteredIntermediateResultString = null;
		String gameName = value.getGame().toString().toLowerCase();
		gameName = Normalizer.normalize(gameName, Normalizer.Form.NFD);
		gameName = gameName.replaceAll("[^a-zA-Z0-9]", "");
		StringTokenizer tokened = new StringTokenizer(gameName);
		LongWritable dateLong = value.getTimeStamp();
		String streamName = value.getDisplayName().toString();
		
		Date date=new Date(value.getTimeStamp().get());
        SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
        		
		String streamDate = df2.format(date);
		String streamOwner = value.getUser().toString();
		/*String uniqueStream = value.getUser().toString();
		uniqueStream = uniqueStream.concat(value.getDisplayName().toString());
		uniqueStream = uniqueStream.concat(date.substring(10));
		String original = uniqueStream;
		StringBuffer sb = new StringBuffer();
		MessageDigest md = null;
		try {
			md = MessageDigest.getInstance("MD5");
			md.update(original.getBytes());
			byte[] digest = md.digest();
			for (byte b : digest) {
				sb.append(String.format("%02x", b & 0xff));
			}
		}catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/

		//get the most popular 100 streams by game type
		while (tokened.hasMoreTokens()){
			switch (tokened.nextToken().toLowerCase()){
			case "leagueoflegends":
				filteredIntermediateResultString = "leagueoflegends" ;break;
			case "minecraft":
				filteredIntermediateResultString = "minecraft" ;break;
			case "worldofwarcraftmistsofpandaria":
				filteredIntermediateResultString = "worldofwarcraftmistsofpandaria" ;break;
			case "counterstrikeglobaloffensive":
				filteredIntermediateResultString = "counterstrikeglobaloffensive" ;break;
			case "callofdutyghosts":
				filteredIntermediateResultString = "callofdutyghosts" ;break;
			case "dota2":
				filteredIntermediateResultString = "dota2" ;break;
			case "battlefield4":
				filteredIntermediateResultString = "battlefield4" ;break;
			case "fifa14":
				filteredIntermediateResultString = "fifa14" ;break;
			case "hearthstoneheroesofwarcraft":
				filteredIntermediateResultString = "hearthstoneheroesofwarcraft" ;break;
			case "diabloiiireaperofsouls":
				filteredIntermediateResultString = "diabloiiireaperofsouls" ;break;
			case "callofdutyblackopsii":
				filteredIntermediateResultString = "callofdutyblackopsii" ;break;
			case "dayz":
				filteredIntermediateResultString = "dayz" ;break;
			case "titanfall":
				filteredIntermediateResultString = "titanfall" ;break;
			case "smite":
				filteredIntermediateResultString = "smite" ;break;
			case "watchdogsa":
				filteredIntermediateResultString = "watchdogsa" ;break;
			case "starcraftiiheartoftheswarm":
				filteredIntermediateResultString = "starcraftiiheartoftheswarm" ;break;
			case "darksoulsii":
				filteredIntermediateResultString = "darksoulsii" ;break;
			case "runescape":
				filteredIntermediateResultString = "runescape" ;break;
			case "nba2k14":
				filteredIntermediateResultString = "nba2k14" ;break;
			case "worldoftanks":
				filteredIntermediateResultString = "worldoftanks" ;break;
			case "armaiii":
				filteredIntermediateResultString = "armaiii" ;break;
			case "diabloiii":
				filteredIntermediateResultString = "diabloiii" ;break;
			case "destiny":
				filteredIntermediateResultString = "destiny" ;break;
			case "wildstar":
				filteredIntermediateResultString = "wildstar" ;break;
			case "pathofexile":
				filteredIntermediateResultString = "pathofexile" ;break;
			case "outlast":
				filteredIntermediateResultString = "outlast" ;break;
			case "thelastofusaremastered":
				filteredIntermediateResultString = "thelastofusaremastered" ;break;
			case "finalfantasyxivonlinearealmreborn":
				filteredIntermediateResultString = "finalfantasyxivonlinearealmreborn" ;break;
			case "osu":
				filteredIntermediateResultString = "osu" ;break;
			case "theelderscrollsonline":
				filteredIntermediateResultString = "theelderscrollsonline" ;break;
			case "grandtheftautov":
				filteredIntermediateResultString = "grandtheftautov" ;break;
			case "warframe":
				filteredIntermediateResultString = "warframe" ;break;
			case "destinybeta":
				filteredIntermediateResultString = "destinybeta" ;break;
			case "maddennfl25":
				filteredIntermediateResultString = "maddennfl25" ;break;
			case "assassinscreedivblackflag":
				filteredIntermediateResultString = "assassinscreedivblackflag" ;break;
			case "darksouls":
				filteredIntermediateResultString = "darksouls" ;break;
			case "finalfantasyxivarealmreborn":
				filteredIntermediateResultString = "finalfantasyxivarealmreborn" ;break;
			case "guildwars2":
				filteredIntermediateResultString = "guildwars2" ;break;
			case "teamfortress2":
				filteredIntermediateResultString = "teamfortress2" ;break;
			case "archeage":
				filteredIntermediateResultString = "archeage" ;break;
			case "rust":
				filteredIntermediateResultString = "rust" ;break;
			case "worldofwarcraft":
				filteredIntermediateResultString = "worldofwarcraft" ;break;
			case "theelderscrollsvskyrim":
				filteredIntermediateResultString = "theelderscrollsvskyrim" ;break;
			case "infestationsurvivorstories":
				filteredIntermediateResultString = "infestationsurvivorstories" ;break;
			case "garrysmod":
				filteredIntermediateResultString = "garrysmod" ;break;
			case "watchdogs":
				filteredIntermediateResultString = "watchdogs" ;break;
			case "infamoussecondsona":
				filteredIntermediateResultString = "infamoussecondsona" ;break;
			case "borderlands2":
				filteredIntermediateResultString = "borderlands2" ;break;
			case "heroesofthestorm":
				filteredIntermediateResultString = "heroesofthestorm" ;break;
			case "callofdutyghostsgoldedition":
				filteredIntermediateResultString = "callofdutyghostsgoldedition" ;break;
			case "warthunder":
				filteredIntermediateResultString = "warthunder" ;break;
			case "callofdutyaghosts":
				filteredIntermediateResultString = "callofdutyaghosts" ;break;
			case "payday2":
				filteredIntermediateResultString = "payday2" ;break;
			case "dcuniverseonline":
				filteredIntermediateResultString = "dcuniverseonline" ;break;
			case "forzamotorsport5":
				filteredIntermediateResultString = "forzamotorsport5" ;break;
			case "maddennfl15":
				filteredIntermediateResultString = "maddennfl15" ;break;
			case "tera":
				filteredIntermediateResultString = "tera" ;break;
			case "starwarstheoldrepublic":
				filteredIntermediateResultString = "starwarstheoldrepublic" ;break;
			case "thebindingofisaac":
				filteredIntermediateResultString = "thebindingofisaac" ;break;
			case "pokamonxy":
				filteredIntermediateResultString = "pokamonxy" ;break;
			case "planetside2":
				filteredIntermediateResultString = "planetside2" ;break;
			case "trialsfusion":
				filteredIntermediateResultString = "trialsfusion" ;break;
			case "heroesofnewerth":
				filteredIntermediateResultString = "heroesofnewerth" ;break;
			case "plantsvszombiesgardenwarfare":
				filteredIntermediateResultString = "plantsvszombiesgardenwarfare" ;break;
			case "apbreloaded":
				filteredIntermediateResultString = "apbreloaded" ;break;
			case "eurotrucksimulator2":
				filteredIntermediateResultString = "eurotrucksimulator2" ;break;
			case "halo3":
				filteredIntermediateResultString = "halo3" ;break;
			case "southparkthestickoftruth":
				filteredIntermediateResultString = "southparkthestickoftruth" ;break;
			case "easportsaufca":
				filteredIntermediateResultString = "easportsaufca" ;break;
			case "metalgearsolidvgroundzeroes":
				filteredIntermediateResultString = "metalgearsolidvgroundzeroes" ;break;
			case "thief":
				filteredIntermediateResultString = "thief" ;break;
			case "unturned":
				filteredIntermediateResultString = "unturned" ;break;
			case "left4dead2":
				filteredIntermediateResultString = "left4dead2" ;break;
			case "infamousasecondson":
				filteredIntermediateResultString = "infamousasecondson" ;break;
			case "clashofclans":
				filteredIntermediateResultString = "clashofclans" ;break;
			case "supermario64":
				filteredIntermediateResultString = "supermario64" ;break;
			case "magicthegathering":
				filteredIntermediateResultString = "magicthegathering" ;break;
			case "mlba14theshowa":
				filteredIntermediateResultString = "mlba14theshowa" ;break;
			case "dawngate":
				filteredIntermediateResultString = "dawngate" ;break;
			case "kerbalspaceprogram":
				filteredIntermediateResultString = "kerbalspaceprogram" ;break;
			case "thelegendofzeldaocarinaoftime":
				filteredIntermediateResultString = "thelegendofzeldaocarinaoftime" ;break;
			case "deadrising3":
				filteredIntermediateResultString = "deadrising3" ;break;
			case "pt":
				filteredIntermediateResultString = "pt" ;break;
			case "battlefield3":
				filteredIntermediateResultString = "battlefield3" ;break;
			case "diabloiiireaperofsoulsaultimateeviledition":
				filteredIntermediateResultString = "diabloiiireaperofsoulsaultimateeviledition" ;break;
			case "needforspeedrivals":
				filteredIntermediateResultString = "needforspeedrivals" ;break;
			case "deadnationaapocalypseedition":
				filteredIntermediateResultString = "deadnationaapocalypseedition" ;break;
			case "destinyfirstlookalpha":
				filteredIntermediateResultString = "destinyfirstlookalpha" ;break;
			case "mygreatgame":
				filteredIntermediateResultString = "mygreatgame" ;break;
			case "mariokart8":
				filteredIntermediateResultString = "mariokart8" ;break;
			case "terraria":
				filteredIntermediateResultString = "terraria" ;break;
			case "wolfensteintheneworder":
				filteredIntermediateResultString = "wolfensteintheneworder" ;break;
			case "eveonline":
				filteredIntermediateResultString = "eveonline" ;break;
			case "tombraiderdefinitiveedition":
				filteredIntermediateResultString = "tombraiderdefinitiveedition" ;break;
			case "killzoneshadowfall":
				filteredIntermediateResultString = "killzoneshadowfall" ;break;
			case "diabloiiireaperofsoulsaultimateevileditionenglish":
				filteredIntermediateResultString = "diabloiiireaperofsoulsaultimateevileditionenglish" ;break;
			case "dontstarveconsoleedition":
				filteredIntermediateResultString = "dontstarveconsoleedition" ;break;
			case "companyofheroes2":
				filteredIntermediateResultString = "companyofheroes2" ;break;
			case "sniperelite3":
				filteredIntermediateResultString = "sniperelite3" ;break;
			case "killerinstinct":
				filteredIntermediateResultString = "killerinstinct" ;break;
			default:
			}
		}
		
		filteredIntermediateResult.set(filteredIntermediateResultString);
		Text bigKey = new Text(filteredIntermediateResult);//+"\t"+streamOwner+"\t"+streamName+"\t"+streamDate);

		context.write(bigKey,dateLong);

	}
}