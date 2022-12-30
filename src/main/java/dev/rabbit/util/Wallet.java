package dev.rabbit.util;

import dev.rabbit.exception.RetrieveBalanceFailure;
import lombok.AccessLevel;
import lombok.Getter;
import org.web3j.crypto.Bip32ECKeyPair;
import org.web3j.crypto.Credentials;
import org.web3j.crypto.ECKeyPair;
import org.web3j.crypto.MnemonicUtils;
import org.web3j.protocol.core.DefaultBlockParameter;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class Wallet {
    private final String seedPhrase;
    @Getter(AccessLevel.PRIVATE)
    private final List<Account> accounts = new ArrayList<>();

    private Wallet(String seedPhrase) {
        this.seedPhrase = seedPhrase;

        // Generate a BIP32 master keypair from the mnemonic phrase
        Bip32ECKeyPair masterKeypair = Bip32ECKeyPair.generateKeyPair(MnemonicUtils.generateSeed(seedPhrase, null));

        for (int i = 0; true; i++) {
            Account account = new Account(Bip32ECKeyPair.deriveKeyPair(masterKeypair, getDerivativePath(i)));
            if (account.getLastKnownBalance().compareTo(new BigInteger("0")) == 0) break;
            accounts.add(account);
        }
    }

    private Wallet(String seedPhrase, int numAccounts) {
        this.seedPhrase = seedPhrase;

        // Generate a BIP32 master keypair from the mnemonic phrase
        Bip32ECKeyPair masterKeypair = Bip32ECKeyPair.generateKeyPair(MnemonicUtils.generateSeed(seedPhrase, null));

        for (int i = 0; i < numAccounts; i++) {
            accounts.add(new Account(Bip32ECKeyPair.deriveKeyPair(masterKeypair, getDerivativePath(i))));
        }
    }

    public static Wallet fromSeed(String seedPhrase) {
        return new Wallet(seedPhrase);
    }

    public static Wallet fromSeed(String seedPhrase, int numAccounts) {
        return new Wallet(seedPhrase, numAccounts);
    }

    private static int[] getDerivativePath(int index) {
        // Derivation path wanted: // m/44'/60'/0'/0
        return new int[]{44 | Bip32ECKeyPair.HARDENED_BIT, 60 | Bip32ECKeyPair.HARDENED_BIT, Bip32ECKeyPair.HARDENED_BIT, 0, index};
    }

    public Account getAccount(int index) {
        return this.getAccounts().get(index);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Seed Phrase: ").append(seedPhrase).append("\n");
        for (int i = 0; i < accounts.size(); i++) {
            sb.append("Account ").append(i).append(" : ").append(accounts.get(i).toString()).append("\n");
        }
        return sb.toString();
    }

    @Getter
    public static class Account {
        private final String address;
        private final String privateKey;
        private final Credentials credentials;
        private final BigInteger lastKnownBalance;

        private Account(ECKeyPair keyPair) {
            this.credentials = Credentials.create(keyPair);
            this.privateKey = keyPair.getPrivateKey().toString(16);
            this.address = this.credentials.getAddress();
            try {
                lastKnownBalance = Web3.getInstance().ethGetBalance(this.address, DefaultBlockParameter.valueOf("latest")).send().getBalance();
            } catch (IOException e) {
                throw new RetrieveBalanceFailure("Failed to retrieve the balance of the account: " + this.address, e);
            }
        }

        @Override
        public String toString() {
            return "Address: " + address + ", PrivateKey: " + privateKey + ", Last Known Balance: " + lastKnownBalance;
        }
    }
}
