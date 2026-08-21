package dev.vality.exporter.businessmetrics.dominant.loader;

import dev.vality.damsel.domain.Reference;
import dev.vality.damsel.domain.TerminalRef;
import dev.vality.damsel.domain_config_v2.Head;
import dev.vality.damsel.domain_config_v2.RepositoryClientSrv;
import dev.vality.damsel.domain_config_v2.VersionReference;
import dev.vality.damsel.domain_config_v2.VersionedObject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;

@Slf4j
@RequiredArgsConstructor
public class TerminalLoader extends DominantLoader<Integer> {

    private final RepositoryClientSrv.Iface dominantClient;

    @Override
    protected String load(Integer terminalId) {
        try {
            Reference reference = new Reference();
            reference.setTerminal(new TerminalRef(terminalId));
            VersionedObject object = dominantClient.checkoutObject(
                    VersionReference.head(new Head()),
                    reference
            );
            return object.getObject()
                    .getTerminal()
                    .getData()
                    .getName();
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String logName() {
        return "terminal";
    }
}
