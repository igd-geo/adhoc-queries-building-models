/*
 * BoyerMooreHorspoolRaita.java
 *
 * Created on 15.09.2003
 *
 * StringSearch - high-performance pattern matching algorithms in Java
 * Copyright (c) 2003-2015 Johann Burkard (<http://johannburkard.de>)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
 * USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */

import java.util.Arrays;

/*
 ******************************************************************************
 * This is a modified version of com.eaio.stringsearch.BoyerMooreHorspoolRaita
 * from https://github.com/elefana/stringsearch released under the MIT license.
 *
 * This modified version accepts `FileBuffer` objects as text and does not
 * depend on other classes.
 ******************************************************************************
 */

/**
 * An implementation of Raita's enhancement to the Boyer-Moore-Horspool String
 * searching algorithm. See "Tuning the Boyer-Moore-Horspool string searching
 * algorithm" (appeared in <em>Software - Practice &amp; Experience,
 * 22(10):879-884</em>).
 * <p>
 * This algorithm is slightly faster than the
 * com.eaio.stringsearch.BoyerMooreHorspool algorithm for the
 * <code>searchChars</code> and <code>searchString</code> methods. It's
 * <code>searchBytes</code> methods are slightly slower.
 *
 * @see <a href="http://johannburkard.de/software/stringsearch/">StringSearch
 * &#8211; high-performance pattern matching algorithms in Java</a>
 * @see <a
 * href="http://johannburkard.de/documents/spe787tr.pdf" target="_top">
 * http://johannburkard.de/documents/spe787tr.pdf</a>
 * @author <a href="http://johannburkard.de">Johann Burkard</a>
 * @version $Id: BoyerMooreHorspoolRaita.java 6675 2015-01-17 21:02:35Z johann $
 */
public class BoyerMooreHorspoolRaita {
    /**
     * Interprets the given <code>byte</code> as an <code>unsigned byte</code>.
     *
     * @param idx the <code>byte</code>
     * @return <code>int</code>
     */
    private static int index(byte idx) {
        /* Much faster in IBM, see com.eaio.stringsearch.performanceTest.Index. */
        /* And MUCH faster in Sun, too. */
        return idx & 0x000000ff;
    }

    public static int[] processBytes(byte[] pattern) {
        if (pattern.length == 1 || pattern.length == 2) {
            return null;
        }

        int[] skip = new int[256];
        Arrays.fill(skip, pattern.length);

        for (int i = 0; i < pattern.length - 1; ++i) {
            skip[index(pattern[i])] = pattern.length - i - 1;
        }

        return skip;
    }

    public static long searchBytes(FileBuffer text, long textStart, long textEnd,
            byte[] pattern, int[] processedPattern) {
        
        // Unrolled fast paths for patterns of length 1 and 2. Suggested by someone who doesn't want to be named.
        
        if (pattern.length == 1) {
            final long nLimit = Math.min(text.getSize(), textEnd);
            for (long n = textStart; n < nLimit; n++) {
                if (text.get(n) == pattern[0])
                    return n;
            }
            return -1;
        }
        else if (pattern.length == 2) {
            final long nLimit = Math.min(text.getSize(), textEnd) - 1;
            for (long n = textStart; n < nLimit; n++) {
                if (text.get(n) == pattern[0]) {
                    if (text.get(n + 1) == pattern[1])
                        return n;
                }
            }
            return -1;
        }

        long i, j, k, mMinusOne;
        byte last, first;

        i = pattern.length - 1;
        mMinusOne = pattern.length - 2;

        last = pattern[pattern.length - 1];
        first = pattern[0];

        i += textStart;

        while (i < textEnd) {

            if (text.get(i) == last && text.get(i - (pattern.length - 1)) == first) {

                k = i - 1;
                j = mMinusOne;

                while (k > -1 && j > -1 && text.get(k) == pattern[(int)j]) {
                    --k;
                    --j;
                }
                if (j == -1) {
                    return k + 1;
                }

            }

            i += processedPattern[index(text.get(i))];
        }

        return -1;
    }
}
