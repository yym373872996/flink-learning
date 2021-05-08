package person.rulo.flink.learning.dstreamapi.serialize;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import person.rulo.flink.learning.dstreamapi.proto.PushMessageProto;

import java.io.IOException;

/**
 * @Author rulo
 * @Date 2021/5/6 20:53
 */
public class ProtobufDeserializationSchema extends AbstractDeserializationSchema<PushMessageProto.PushMessageMq> {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public PushMessageProto.PushMessageMq deserialize(byte[] bytes) throws IOException {
        PushMessageProto.PushMessageMq pushMessageMq = null;
        try {
            pushMessageMq = PushMessageProto.PushMessageMq.parseFrom(bytes);
        } catch (Exception e) {
            if (logger.isWarnEnabled()) {
                logger.warn("deserialize error: {}", e);
            }
        }
        return pushMessageMq;
    }
}
