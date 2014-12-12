package dexter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.l3s.lemma.lemma;
import edu.stanford.nlp.ling.tokensregex.MultiWordStringMatcher;
import edu.stanford.nlp.ling.tokensregex.MultiWordStringMatcher.MatchType;

// \E(?:\p{Punct}|\s)*\Q
public class Misc {

	public static void main(String[] args) {
		
		//lemma.init();
		
		/*String s = new MultiWordStringMatcher(MatchType.LNRM).getLnrmRegex(lemma.getLemmatization("Alice MOVIEs, % $:<'          "));
		 System.out.println(s);
		    String x = new MultiWordStringMatcher(MatchType.LNRM).getLnrmRegex(lemma.getLemmatization("alicemovie"));
	System.out.println(x);
	     Pattern p = Pattern.compile(s);
		 Matcher m = p.matcher("alice_(movie)");
		     System.out.println(m.matches());
	    System.out.println(m.find()); 	*/
		
		System.out.println('8'-'0');
	}
}
