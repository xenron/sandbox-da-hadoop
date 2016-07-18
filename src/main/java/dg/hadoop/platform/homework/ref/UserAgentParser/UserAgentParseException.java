package dg.hadoop.platform.homework.ref.UserAgentParser;

/**
 * Catches scenarios where user agent cannot be parsed.
 * @author Glen Smith (glen@bytecode.com.au)
 */
public class UserAgentParseException extends RuntimeException {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public UserAgentParseException(String message) {
        super(message);
    }

}
