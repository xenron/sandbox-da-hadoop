package com.packt.test;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

public class TweetsAnalyzer extends Analyzer {
	private DoubleMetaphone filter = new DoubleMetaphone();

	@Override
	protected TokenStreamComponents createComponents(String arg0, Reader reader) {
		Tokenizer source;
		
		final TokenStream result = new PorterStemFilter(new StopFilter(Version.LUCENE_CURRENT, new StandardTokenizer(Version.LUCENE_CURRENT, reader),
				StandardAnalyzer.STOP_WORDS_SET));
				CharTermAttribute termAtt = (CharTermAttribute) result.addAttribute(CharTermAttribute.class);
				StringBuilder buf = new StringBuilder();
				try {
				while (result.incrementToken()) {
				String word = new String(termAtt.buffer(), 0, termAtt.length());
				buf.append(filter.encode(word)).append(" ");}
				} catch (IOException e) {
					e.printStackTrace();
					}
				source =  new WhitespaceTokenizer(Version.LUCENE_CURRENT, new StringReader(buf.toString()));
			    return new TokenStreamComponents(source);
				}


}
