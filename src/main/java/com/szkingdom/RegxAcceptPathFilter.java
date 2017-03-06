package com.szkingdom;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * @DESCRIPTION ${DESCRIPTION}
 * @Author TangPeng
 * @Date 2017-3-6
 */
public class RegxAcceptPathFilter implements PathFilter{

    private  final String regex;
    public RegxAcceptPathFilter(String regex) {
        this.regex=regex;
    }

    public boolean accept(Path path) {

        boolean flag=path.toString().matches(regex);
        return flag;
    }
}
