package co.cask.cdap.templates.etl.common.kafka;

public class MessageDecoderException extends RuntimeException {
	private static final long serialVersionUID = -3908501575563624012L;

	public MessageDecoderException(String message) {
		super(message);
	}

	public MessageDecoderException(String message, Exception e) {
		super(message, e);
	}

	public MessageDecoderException(Exception e) {
		super(e);
	}
}