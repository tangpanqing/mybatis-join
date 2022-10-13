package github.tangpanqing.mybatisjoin.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtil {

    public static String underlineToCamel(String str) {
        Matcher matcher = Pattern.compile("_([a-z])").matcher(str);
        StringBuffer sb = new StringBuffer(str);
        if (matcher.find()) {
            sb = new StringBuffer();
            matcher.appendReplacement(sb, matcher.group(1).toUpperCase());
            matcher.appendTail(sb);
        } else {
            return sb.toString().replaceAll("_", "");
        }
        return sb.toString();
    }

    public static String camelToUnderline(String line){
        Pattern humpPattern = Pattern.compile("[A-Z]");
        Matcher matcher = humpPattern.matcher(line);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            matcher.appendReplacement(sb, "_" + matcher.group(0).toLowerCase());
        }
        matcher.appendTail(sb);
        String substring = sb.toString().substring(1);
        return substring;
    }

}
