package task1;

import java.util.Formatter;

/**
 * Created by DmitriyBrosalin on 13/04/2019.
 */
public class CommonUtils {

    public static String byteToHex(final byte[] hash)
    {
        Formatter formatter = new Formatter();
        for (byte b : hash)
        {
            formatter.format("%02x", b);
        }
        String result = formatter.toString();
        formatter.close();
        return result;
    }

}
