package dev.vality.exporter.businessmetrics.dominant.loader;

import dev.vality.damsel.domain.CurrencyRef;
import dev.vality.damsel.domain.Reference;
import dev.vality.damsel.domain_config_v2.Head;
import dev.vality.damsel.domain_config_v2.RepositoryClientSrv;
import dev.vality.damsel.domain_config_v2.VersionReference;
import dev.vality.damsel.domain_config_v2.VersionedObject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;

@Slf4j
@RequiredArgsConstructor
public class CurrencyLoader extends DominantLoader<String> {

    private final RepositoryClientSrv.Iface dominantClient;

    @Override
    protected String load(String currencyCode) {
        try {
            Reference reference = new Reference();
            reference.setCurrency(new CurrencyRef(currencyCode));
            VersionedObject object = dominantClient.checkoutObject(
                    VersionReference.head(new Head()),
                    reference
            );
            return String.valueOf(object.getObject()
                    .getCurrency()
                    .getData()
                    .getExponent());
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String logName() {
        return "currency";
    }
}
