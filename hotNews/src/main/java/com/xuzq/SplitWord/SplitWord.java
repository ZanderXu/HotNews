package com.xuzq.SplitWord;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;

/**
 * Created by xuzq on 2016/4/6.
 */
public class SplitWord {

    public SplitWord()
    {
    }


    public static void main(String[] args) throws IOException {
        String text = "本报讯 基于java语言开发的轻量级的中文分词工具包吐槽";
        System.out.println(splitWordMethod1(text));
        //System.out.println(splitWordMethod1(text).split(" "));
    }
    public  static String splitWordMethod1(String message) throws IOException
    {
        StringBuffer resultMessage = new StringBuffer();
        IKSegmenter ikS = new IKSegmenter(new StringReader(message), true);
        Lexeme lex = null;
        while((lex = ikS.next())!= null){
            resultMessage.append(lex.getLexemeText() + " ");
        }
        return resultMessage.toString();
    }

    //split message with stopword.dic
    public  String splitWordMethod(String message) throws IOException
    {
        StringBuffer resultMessage = new StringBuffer();
        IKSegmenter ikS = new IKSegmenter(new StringReader(message), true);
        Lexeme lex = null;
        while((lex = ikS.next())!= null){
            resultMessage.append(lex.getLexemeText() + " ");
        }
        return resultMessage.toString();
    }
}
