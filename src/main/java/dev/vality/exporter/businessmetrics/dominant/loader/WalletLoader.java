package dev.vality.exporter.businessmetrics.dominant.loader;

import dev.vality.damsel.domain.Reference;
import dev.vality.damsel.domain.WalletConfigRef;
import dev.vality.damsel.domain_config_v2.Head;
import dev.vality.damsel.domain_config_v2.RepositoryClientSrv;
import dev.vality.damsel.domain_config_v2.VersionReference;
import dev.vality.damsel.domain_config_v2.VersionedObject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;

@Slf4j
@RequiredArgsConstructor
public class WalletLoader extends DominantLoader<String> {

    private final RepositoryClientSrv.Iface dominantClient;

    @Override
    protected String load(String walletId) {
        try {
            Reference reference = new Reference();
            reference.setWalletConfig(new WalletConfigRef(walletId));
            VersionedObject object = dominantClient.checkoutObject(
                    VersionReference.head(new Head()),
                    reference
            );
            return object.getObject()
                    .getWalletConfig()
                    .getData()
                    .getName();
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String logName() {
        return "wallet";
    }
}
