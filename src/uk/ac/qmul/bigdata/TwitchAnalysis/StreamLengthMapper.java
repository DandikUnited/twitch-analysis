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
		String streamOwner = value.getUser().toString();
		
		Date date=new Date(value.getTimeStamp().get());
        SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
        		
		String streamDate = df2.format(date);
		
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
			switch (tokened.nextToken()){
			case "leagueoflegends":
				filteredIntermediateResultString = "leagueoflegends"; filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "minecraft":
				filteredIntermediateResultString = "minecraft" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "worldofwarcraftmistsofpandaria":
				filteredIntermediateResultString = "worldofwarcraftmistsofpandaria" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "counterstrikeglobaloffensive":
				filteredIntermediateResultString = "counterstrikeglobaloffensive" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "callofdutyghosts":
				filteredIntermediateResultString = "callofdutyghosts" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "dota2":
				filteredIntermediateResultString = "dota2" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "battlefield4":
				filteredIntermediateResultString = "battlefield4" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "fifa14":
				filteredIntermediateResultString = "fifa14" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "hearthstoneheroesofwarcraft":
				filteredIntermediateResultString = "hearthstoneheroesofwarcraft" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "diabloiiireaperofsouls":
				filteredIntermediateResultString = "diabloiiireaperofsouls" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "callofdutyblackopsii":
				filteredIntermediateResultString = "callofdutyblackopsii" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "dayz":
				filteredIntermediateResultString = "dayz" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "titanfall":
				filteredIntermediateResultString = "titanfall" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "smite":
				filteredIntermediateResultString = "smite" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "watchdogsa":
				filteredIntermediateResultString = "watchdogsa" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "starcraftiiheartoftheswarm":
				filteredIntermediateResultString = "starcraftiiheartoftheswarm" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "darksoulsii":
				filteredIntermediateResultString = "darksoulsii" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "runescape":
				filteredIntermediateResultString = "runescape" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "nba2k14":
				filteredIntermediateResultString = "nba2k14" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "worldoftanks":
				filteredIntermediateResultString = "worldoftanks" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "armaiii":
				filteredIntermediateResultString = "armaiii" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "diabloiii":
				filteredIntermediateResultString = "diabloiii" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "destiny":
				filteredIntermediateResultString = "destiny" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "wildstar":
				filteredIntermediateResultString = "wildstar" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "pathofexile":
				filteredIntermediateResultString = "pathofexile" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "outlast":
				filteredIntermediateResultString = "outlast" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "thelastofusaremastered":
				filteredIntermediateResultString = "thelastofusaremastered" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "finalfantasyxivonlinearealmreborn":
				filteredIntermediateResultString = "finalfantasyxivonlinearealmreborn" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "osu":
				filteredIntermediateResultString = "osu" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "theelderscrollsonline":
				filteredIntermediateResultString = "theelderscrollsonline" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "grandtheftautov":
				filteredIntermediateResultString = "grandtheftautov" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "warframe":
				filteredIntermediateResultString = "warframe" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "destinybeta":
				filteredIntermediateResultString = "destinybeta" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "maddennfl25":
				filteredIntermediateResultString = "maddennfl25" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "assassinscreedivblackflag":
				filteredIntermediateResultString = "assassinscreedivblackflag" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "darksouls":
				filteredIntermediateResultString = "darksouls" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "finalfantasyxivarealmreborn":
				filteredIntermediateResultString = "finalfantasyxivarealmreborn" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "guildwars2":
				filteredIntermediateResultString = "guildwars2" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "teamfortress2":
				filteredIntermediateResultString = "teamfortress2" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "archeage":
				filteredIntermediateResultString = "archeage" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "rust":
				filteredIntermediateResultString = "rust" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "worldofwarcraft":
				filteredIntermediateResultString = "worldofwarcraft" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "theelderscrollsvskyrim":
				filteredIntermediateResultString = "theelderscrollsvskyrim" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "infestationsurvivorstories":
				filteredIntermediateResultString = "infestationsurvivorstories" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "garrysmod":
				filteredIntermediateResultString = "garrysmod" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "watchdogs":
				filteredIntermediateResultString = "watchdogs" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "infamoussecondsona":
				filteredIntermediateResultString = "infamoussecondsona" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "borderlands2":
				filteredIntermediateResultString = "borderlands2" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "heroesofthestorm":
				filteredIntermediateResultString = "heroesofthestorm" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "callofdutyghostsgoldedition":
				filteredIntermediateResultString = "callofdutyghostsgoldedition" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "warthunder":
				filteredIntermediateResultString = "warthunder" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "callofdutyaghosts":
				filteredIntermediateResultString = "callofdutyaghosts" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "payday2":
				filteredIntermediateResultString = "payday2" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "dcuniverseonline":
				filteredIntermediateResultString = "dcuniverseonline" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "forzamotorsport5":
				filteredIntermediateResultString = "forzamotorsport5" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "maddennfl15":
				filteredIntermediateResultString = "maddennfl15" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "tera":
				filteredIntermediateResultString = "tera" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "starwarstheoldrepublic":
				filteredIntermediateResultString = "starwarstheoldrepublic" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "thebindingofisaac":
				filteredIntermediateResultString = "thebindingofisaac" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "pokamonxy":
				filteredIntermediateResultString = "pokamonxy" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "planetside2":
				filteredIntermediateResultString = "planetside2" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "trialsfusion":
				filteredIntermediateResultString = "trialsfusion" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "heroesofnewerth":
				filteredIntermediateResultString = "heroesofnewerth" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "plantsvszombiesgardenwarfare":
				filteredIntermediateResultString = "plantsvszombiesgardenwarfare" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "apbreloaded":
				filteredIntermediateResultString = "apbreloaded" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "eurotrucksimulator2":
				filteredIntermediateResultString = "eurotrucksimulator2" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "halo3":
				filteredIntermediateResultString = "halo3" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "southparkthestickoftruth":
				filteredIntermediateResultString = "southparkthestickoftruth" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "easportsaufca":
				filteredIntermediateResultString = "easportsaufca" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "metalgearsolidvgroundzeroes":
				filteredIntermediateResultString = "metalgearsolidvgroundzeroes" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "thief":
				filteredIntermediateResultString = "thief" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "unturned":
				filteredIntermediateResultString = "unturned" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "left4dead2":
				filteredIntermediateResultString = "left4dead2" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "infamousasecondson":
				filteredIntermediateResultString = "infamousasecondson" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "clashofclans":
				filteredIntermediateResultString = "clashofclans" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "supermario64":
				filteredIntermediateResultString = "supermario64" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "magicthegathering":
				filteredIntermediateResultString = "magicthegathering" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "mlba14theshowa":
				filteredIntermediateResultString = "mlba14theshowa" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "dawngate":
				filteredIntermediateResultString = "dawngate" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "kerbalspaceprogram":
				filteredIntermediateResultString = "kerbalspaceprogram" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "thelegendofzeldaocarinaoftime":
				filteredIntermediateResultString = "thelegendofzeldaocarinaoftime" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "deadrising3":
				filteredIntermediateResultString = "deadrising3" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "pt":
				filteredIntermediateResultString = "pt" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "battlefield3":
				filteredIntermediateResultString = "battlefield3" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "diabloiiireaperofsoulsaultimateeviledition":
				filteredIntermediateResultString = "diabloiiireaperofsoulsaultimateeviledition" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "needforspeedrivals":
				filteredIntermediateResultString = "needforspeedrivals" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "deadnationaapocalypseedition":
				filteredIntermediateResultString = "deadnationaapocalypseedition" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "destinyfirstlookalpha":
				filteredIntermediateResultString = "destinyfirstlookalpha" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "mygreatgame":
				filteredIntermediateResultString = "mygreatgame" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "mariokart8":
				filteredIntermediateResultString = "mariokart8" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "terraria":
				filteredIntermediateResultString = "terraria" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "wolfensteintheneworder":
				filteredIntermediateResultString = "wolfensteintheneworder" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "eveonline":
				filteredIntermediateResultString = "eveonline" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "tombraiderdefinitiveedition":
				filteredIntermediateResultString = "tombraiderdefinitiveedition" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "killzoneshadowfall":
				filteredIntermediateResultString = "killzoneshadowfall" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "diabloiiireaperofsoulsaultimateevileditionenglish":
				filteredIntermediateResultString = "diabloiiireaperofsoulsaultimateevileditionenglish" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "dontstarveconsoleedition":
				filteredIntermediateResultString = "dontstarveconsoleedition" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "companyofheroes2":
				filteredIntermediateResultString = "companyofheroes2" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "sniperelite3":
				filteredIntermediateResultString = "sniperelite3" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			case "killerinstinct":
				filteredIntermediateResultString = "killerinstinct" ;filteredIntermediateResult.set(filteredIntermediateResultString);break;
			default:
			}
			
		}
		
		
		Text bigKey = new Text(filteredIntermediateResultString+"\t"+streamOwner+"\t"+streamName+"\t"+streamDate);

		context.write(bigKey,dateLong);

	}
}