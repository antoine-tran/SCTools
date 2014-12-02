package dexter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.l3s.lemma.lemma;
import edu.stanford.nlp.ling.tokensregex.MultiWordStringMatcher;
import edu.stanford.nlp.ling.tokensregex.MultiWordStringMatcher.MatchType;

// \E(?:\p{Punct}|\s)*\Q
public class Misc {

	public static void main(String[] args) {
		
		lemma.init();
		
		String s = new MultiWordStringMatcher(MatchType.LNRM).getLnrmRegex(lemma.getLemmatization("Alice MOVIEs, % $:<'          "));
		/* 360 */     System.out.println(s);
		/* 361 */     String x = new MultiWordStringMatcher(MatchType.LNRM).getLnrmRegex(lemma.getLemmatization("alicemovie"));
		/* 362 */     System.out.println(x);
		/* 363 */     Pattern p = Pattern.compile(s);
		/* 364 */     Matcher m = p.matcher("alice_(movie)");
		/* 365 */     System.out.println(m.matches());
		/* 366 */     System.out.println(m.find());	
	}
}
