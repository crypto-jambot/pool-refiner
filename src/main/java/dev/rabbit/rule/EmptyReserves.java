package dev.rabbit.rule;

import com.google.api.services.bigquery.model.TableRow;
import dev.rabbit.exception.RetrieveBalanceFailure;
import dev.rabbit.generated.IERC20;
import dev.rabbit.util.Web3;
import org.web3j.abi.FunctionEncoder;
import org.web3j.abi.FunctionReturnDecoder;
import org.web3j.abi.TypeReference;
import org.web3j.abi.datatypes.Address;
import org.web3j.abi.datatypes.Function;
import org.web3j.abi.datatypes.Type;
import org.web3j.abi.datatypes.generated.Uint256;
import org.web3j.abi.datatypes.generated.Uint8;
import org.web3j.protocol.core.BatchRequest;
import org.web3j.protocol.core.DefaultBlockParameterName;
import org.web3j.protocol.core.Request;
import org.web3j.protocol.core.Response;
import org.web3j.protocol.core.methods.request.Transaction;
import org.web3j.protocol.core.methods.response.EthCall;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static dev.rabbit.util.Constants.NULL_ADDRESS;
import static dev.rabbit.util.ListUtils.toList;
import static java.math.BigInteger.ONE;
import static java.math.BigInteger.TEN;


public class EmptyReserves implements Rule {

    private static final Function DECIMALS_FN = new Function(IERC20.FUNC_DECIMALS, List.of(), List.of(new TypeReference<Uint8>() {
    }));

    private static final String DECIMALS_ENCODED_FN = FunctionEncoder.encode(DECIMALS_FN);

    private static String getBalanceOfEncodedFn(String address) {
        return FunctionEncoder.encode(getBalanceOfFn(address));
    }

    private static Function getBalanceOfFn(String address) {
        return new Function(IERC20.FUNC_BALANCEOF, List.of(new Address(160, address)), List.of(new TypeReference<Uint256>() {
        }));
    }

    @Override
    public FilterType getFilterType() {
        return FilterType.BATCH;
    }

    @Override
    public Iterable<TableRow> applyRule(Iterable<TableRow> elements) {
        List<TableRow> listElements = toList(elements);
        List<TableRow> toBeOutput = new ArrayList<>();
        BatchRequest batchRequest = new BatchRequest(Web3.getHttpService());

        for (int i = 0; i < listElements.size(); i++) {
            TableRow row = listElements.get(i);
            String poolAddress = (String) row.get("pool");
            String token0 = (String) row.get("token0Address");
            String token1 = (String) row.get("token1Address");
            List<Request<?, EthCall>> requests = List.of(
                    Web3.getInstance().ethCall(Transaction.createEthCallTransaction(NULL_ADDRESS, token0, getBalanceOfEncodedFn(poolAddress)), DefaultBlockParameterName.LATEST),
                    Web3.getInstance().ethCall(Transaction.createEthCallTransaction(NULL_ADDRESS, token1, getBalanceOfEncodedFn(poolAddress)), DefaultBlockParameterName.LATEST),
                    Web3.getInstance().ethCall(Transaction.createEthCallTransaction(NULL_ADDRESS, token0, DECIMALS_ENCODED_FN), DefaultBlockParameterName.LATEST),
                    Web3.getInstance().ethCall(Transaction.createEthCallTransaction(NULL_ADDRESS, token1, DECIMALS_ENCODED_FN), DefaultBlockParameterName.LATEST));
            for (Request<?, EthCall> request : requests) {
                request.setId(i);
                batchRequest.add(request);
            }
        }
        List<? extends Response<?>> responses;
        try {
            responses = batchRequest.send().getResponses();

        } catch (IOException exception) {
            throw new RetrieveBalanceFailure("Failed to retrieve balances for pools", exception);
        }
        Map<Integer, List<Response<?>>> idToResponseMap = groupResponsesById(responses);
        for (Map.Entry<Integer, List<Response<?>>> entry : idToResponseMap.entrySet()) {
            TableRow tableRow = listElements.get(entry.getKey());

            if (entry.getValue().stream().anyMatch(Response::hasError)) continue;
            if (entry.getValue().stream().anyMatch(response -> ((String) response.getResult()).equalsIgnoreCase("0x") || ((String) response.getResult()).isEmpty()))
                continue;

            Response<?> token0Response = entry.getValue().get(0);
            List<Type> token0ResponseOutputTypes = FunctionReturnDecoder.decode((String) token0Response.getResult(), getBalanceOfFn((String) tableRow.get("pool")).getOutputParameters());
            BigInteger token0Balance = (BigInteger) token0ResponseOutputTypes.get(0).getValue();

            Response<?> token1Response = entry.getValue().get(1);
            List<Type> token1ResponseOutputTypes = FunctionReturnDecoder.decode((String) token1Response.getResult(), getBalanceOfFn((String) tableRow.get("pool")).getOutputParameters());
            BigInteger token1Balance = (BigInteger) token1ResponseOutputTypes.get(0).getValue();

            Response<?> token0DecimalsResponse = entry.getValue().get(2);
            List<Type> token0DecimalsResponseOutputTypes = FunctionReturnDecoder.decode((String) token0DecimalsResponse.getResult(), DECIMALS_FN.getOutputParameters());
            BigInteger token0Decimals = (BigInteger) token0DecimalsResponseOutputTypes.get(0).getValue();

            Response<?> token1DecimalsResponse = entry.getValue().get(3);
            List<Type> token1DecimalsResponseOutputTypes = FunctionReturnDecoder.decode((String) token1DecimalsResponse.getResult(), DECIMALS_FN.getOutputParameters());
            BigInteger token1Decimals = (BigInteger) token1DecimalsResponseOutputTypes.get(0).getValue();

            BigInteger poolBalanceOfToken0 = token0Balance.divide(TEN.pow(token0Decimals.intValue()));
            BigInteger poolBalanceOfToken1 = token1Balance.divide(TEN.pow(token1Decimals.intValue()));

            if (poolBalanceOfToken0.compareTo(ONE) >= 1 && poolBalanceOfToken1.compareTo(ONE) >= 1)
                toBeOutput.add(tableRow);
        }
        System.out.println("Input was " + listElements.size() + " but only output " + toBeOutput.size());
        return toBeOutput;
    }

    private Map<Integer, List<Response<?>>> groupResponsesById(List<? extends Response<?>> responses) {
        Map<Integer, List<Response<?>>> idToResponseMap = new HashMap<>();
        for (Response<?> response : responses) {
            int id = (int) response.getId();
            List<Response<?>> idResponses = idToResponseMap.getOrDefault(id, new ArrayList<>());
            idResponses.add(response);
            idToResponseMap.put(id, idResponses);
        }
        return idToResponseMap;
    }
}
