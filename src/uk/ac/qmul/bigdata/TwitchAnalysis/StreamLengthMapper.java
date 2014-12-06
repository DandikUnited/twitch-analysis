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
Mapper<Object, TwitchDataRecord, Text, IntWritable> {

	public void map(Object key, TwitchDataRecord value, Context context)
			throws IOException, InterruptedException {
		Text filteredIntermediateResult = new Text();
		String gameName = value.getGame().toString().toLowerCase();
		gameName = Normalizer.normalize(gameName, Normalizer.Form.NFD);
		gameName = gameName.replaceAll("[^a-zA-Z0-9]", "");
		StringTokenizer tokened = new StringTokenizer(gameName);
		String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(value.getTimeStamp().get()));
		String uniqueStream = value.getUser().toString();
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
		}

		//get the most popular 100 streams by game type
		while (tokened.hasMoreTokens()){
			switch (tokened.nextToken().toLowerCase()){
			case "leagueoflegends":
				filteredIntermediateResult.set("leagueoflegends");break;
			case "minecraft":
				filteredIntermediateResult.set("minecraft");break;
			case "worldofwarcraftmistsofpandaria":
				filteredIntermediateResult.set("worldofwarcraftmistsofpandaria");break;
			case "counterstrikeglobaloffensive":
				filteredIntermediateResult.set("counterstrikeglobaloffensive");break;
			case "callofdutyghosts":
				filteredIntermediateResult.set("callofdutyghosts");break;
			case "dota2":
				filteredIntermediateResult.set("dota2");break;
			case "battlefield4":
				filteredIntermediateResult.set("battlefield4");break;
			case "fifa14":
				filteredIntermediateResult.set("fifa14");break;
			case "hearthstoneheroesofwarcraft":
				filteredIntermediateResult.set("hearthstoneheroesofwarcraft");break;
			case "diabloiiireaperofsouls":
				filteredIntermediateResult.set("diabloiiireaperofsouls");break;
			case "callofdutyblackopsii":
				filteredIntermediateResult.set("callofdutyblackopsii");break;
			case "dayz":
				filteredIntermediateResult.set("dayz");break;
			case "titanfall":
				filteredIntermediateResult.set("titanfall");break;
			case "smite":
				filteredIntermediateResult.set("smite");break;
			case "watchdogsa":
				filteredIntermediateResult.set("watchdogsa");break;
			case "starcraftiiheartoftheswarm":
				filteredIntermediateResult.set("starcraftiiheartoftheswarm");break;
			case "darksoulsii":
				filteredIntermediateResult.set("darksoulsii");break;
			case "runescape":
				filteredIntermediateResult.set("runescape");break;
			case "nba2k14":
				filteredIntermediateResult.set("nba2k14");break;
			case "worldoftanks":
				filteredIntermediateResult.set("worldoftanks");break;
			case "armaiii":
				filteredIntermediateResult.set("armaiii");break;
			case "diabloiii":
				filteredIntermediateResult.set("diabloiii");break;
			case "destiny":
				filteredIntermediateResult.set("destiny");break;
			case "wildstar":
				filteredIntermediateResult.set("wildstar");break;
			case "pathofexile":
				filteredIntermediateResult.set("pathofexile");break;
			case "outlast":
				filteredIntermediateResult.set("outlast");break;
			case "thelastofusaremastered":
				filteredIntermediateResult.set("thelastofusaremastered");break;
			case "finalfantasyxivonlinearealmreborn":
				filteredIntermediateResult.set("finalfantasyxivonlinearealmreborn");break;
			case "osu":
				filteredIntermediateResult.set("osu");break;
			case "theelderscrollsonline":
				filteredIntermediateResult.set("theelderscrollsonline");break;
			case "grandtheftautov":
				filteredIntermediateResult.set("grandtheftautov");break;
			case "warframe":
				filteredIntermediateResult.set("warframe");break;
			case "destinybeta":
				filteredIntermediateResult.set("destinybeta");break;
			case "maddennfl25":
				filteredIntermediateResult.set("maddennfl25");break;
			case "assassinscreedivblackflag":
				filteredIntermediateResult.set("assassinscreedivblackflag");break;
			case "darksouls":
				filteredIntermediateResult.set("darksouls");break;
			case "finalfantasyxivarealmreborn":
				filteredIntermediateResult.set("finalfantasyxivarealmreborn");break;
			case "guildwars2":
				filteredIntermediateResult.set("guildwars2");break;
			case "teamfortress2":
				filteredIntermediateResult.set("teamfortress2");break;
			case "archeage":
				filteredIntermediateResult.set("archeage");break;
			case "rust":
				filteredIntermediateResult.set("rust");break;
			case "worldofwarcraft":
				filteredIntermediateResult.set("worldofwarcraft");break;
			case "theelderscrollsvskyrim":
				filteredIntermediateResult.set("theelderscrollsvskyrim");break;
			case "infestationsurvivorstories":
				filteredIntermediateResult.set("infestationsurvivorstories");break;
			case "garrysmod":
				filteredIntermediateResult.set("garrysmod");break;
			case "watchdogs":
				filteredIntermediateResult.set("watchdogs");break;
			case "infamoussecondsona":
				filteredIntermediateResult.set("infamoussecondsona");break;
			case "borderlands2":
				filteredIntermediateResult.set("borderlands2");break;
			case "heroesofthestorm":
				filteredIntermediateResult.set("heroesofthestorm");break;
			case "callofdutyghostsgoldedition":
				filteredIntermediateResult.set("callofdutyghostsgoldedition");break;
			case "warthunder":
				filteredIntermediateResult.set("warthunder");break;
			case "callofdutyaghosts":
				filteredIntermediateResult.set("callofdutyaghosts");break;
			case "payday2":
				filteredIntermediateResult.set("payday2");break;
			case "dcuniverseonline":
				filteredIntermediateResult.set("dcuniverseonline");break;
			case "forzamotorsport5":
				filteredIntermediateResult.set("forzamotorsport5");break;
			case "maddennfl15":
				filteredIntermediateResult.set("maddennfl15");break;
			case "tera":
				filteredIntermediateResult.set("tera");break;
			case "starwarstheoldrepublic":
				filteredIntermediateResult.set("starwarstheoldrepublic");break;
			case "thebindingofisaac":
				filteredIntermediateResult.set("thebindingofisaac");break;
			case "pokamonxy":
				filteredIntermediateResult.set("pokamonxy");break;
			case "planetside2":
				filteredIntermediateResult.set("planetside2");break;
			case "trialsfusion":
				filteredIntermediateResult.set("trialsfusion");break;
			case "heroesofnewerth":
				filteredIntermediateResult.set("heroesofnewerth");break;
			case "plantsvszombiesgardenwarfare":
				filteredIntermediateResult.set("plantsvszombiesgardenwarfare");break;
			case "apbreloaded":
				filteredIntermediateResult.set("apbreloaded");break;
			case "eurotrucksimulator2":
				filteredIntermediateResult.set("eurotrucksimulator2");break;
			case "halo3":
				filteredIntermediateResult.set("halo3");break;
			case "southparkthestickoftruth":
				filteredIntermediateResult.set("southparkthestickoftruth");break;
			case "easportsaufca":
				filteredIntermediateResult.set("easportsaufca");break;
			case "metalgearsolidvgroundzeroes":
				filteredIntermediateResult.set("metalgearsolidvgroundzeroes");break;
			case "thief":
				filteredIntermediateResult.set("thief");break;
			case "unturned":
				filteredIntermediateResult.set("unturned");break;
			case "left4dead2":
				filteredIntermediateResult.set("left4dead2");break;
			case "infamousasecondson":
				filteredIntermediateResult.set("infamousasecondson");break;
			case "clashofclans":
				filteredIntermediateResult.set("clashofclans");break;
			case "supermario64":
				filteredIntermediateResult.set("supermario64");break;
			case "magicthegathering":
				filteredIntermediateResult.set("magicthegathering");break;
			case "mlba14theshowa":
				filteredIntermediateResult.set("mlba14theshowa");break;
			case "dawngate":
				filteredIntermediateResult.set("dawngate");break;
			case "kerbalspaceprogram":
				filteredIntermediateResult.set("kerbalspaceprogram");break;
			case "thelegendofzeldaocarinaoftime":
				filteredIntermediateResult.set("thelegendofzeldaocarinaoftime");break;
			case "deadrising3":
				filteredIntermediateResult.set("deadrising3");break;
			case "pt":
				filteredIntermediateResult.set("pt");break;
			case "battlefield3":
				filteredIntermediateResult.set("battlefield3");break;
			case "diabloiiireaperofsoulsaultimateeviledition":
				filteredIntermediateResult.set("diabloiiireaperofsoulsaultimateeviledition");break;
			case "needforspeedrivals":
				filteredIntermediateResult.set("needforspeedrivals");break;
			case "deadnationaapocalypseedition":
				filteredIntermediateResult.set("deadnationaapocalypseedition");break;
			case "destinyfirstlookalpha":
				filteredIntermediateResult.set("destinyfirstlookalpha");break;
			case "mygreatgame":
				filteredIntermediateResult.set("mygreatgame");break;
			case "mariokart8":
				filteredIntermediateResult.set("mariokart8");break;
			case "terraria":
				filteredIntermediateResult.set("terraria");break;
			case "wolfensteintheneworder":
				filteredIntermediateResult.set("wolfensteintheneworder");break;
			case "eveonline":
				filteredIntermediateResult.set("eveonline");break;
			case "tombraiderdefinitiveedition":
				filteredIntermediateResult.set("tombraiderdefinitiveedition");break;
			case "killzoneshadowfall":
				filteredIntermediateResult.set("killzoneshadowfall");break;
			case "diabloiiireaperofsoulsaultimateevileditionenglish":
				filteredIntermediateResult.set("diabloiiireaperofsoulsaultimateevileditionenglish");break;
			case "dontstarveconsoleedition":
				filteredIntermediateResult.set("dontstarveconsoleedition");break;
			case "companyofheroes2":
				filteredIntermediateResult.set("companyofheroes2");break;
			case "sniperelite3":
				filteredIntermediateResult.set("sniperelite3");break;
			case "killerinstinct":
				filteredIntermediateResult.set("killerinstinct");break;
			default:	
			}
		}

		context.write(new Text(filteredIntermediateResult.toString().concat(uniqueStream)),new IntWritable(1));

	}
}