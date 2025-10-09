### Address & Illicit Flow Lists
This is a repository for publicly releasing the 686 laundering addresses and 4183 previously undiscovered illicit fund flow records contributed by the paper *Shedding Light on Shadows: Automatically Tracing Illicit Money Flows on EVM-Compatible Blockchains* accepted to SIGMETRICS 2026.

### Discussions

On July 16, 2024, [Li.Fi](https://x.com/lifiprotocol), a cross-chain bridge and DEX aggregator, experienced a significant security breach that exploited the [Li.Fi Diamond Contract](https://etherscan.io/address/0x1231deb6f5749ef6ce6943a275a1d3e7486f4eae). Various stable tokens and other assets of roughly \$ $11.6\mathrm{M}$ were stolen from users. The attacker was able to drain funds from users who had granted infinite approvals to the attacked contract. 

- Attacker’s Address: [0x8b3cb6bf982798fba233bca56749e22eec42dcf3](https://etherscan.io/address/0x8b3cb6bf982798fba233bca56749e22eec42dcf3)
- Vulnerable Contract: [0x1231deb6f5749ef6ce6943a275a1d3e7486f4eae](https://etherscan.io/address/0x1231deb6f5749ef6ce6943a275a1d3e7486f4eae)
- Examples of Attack Transaction: [0xd82f](https://etherscan.io/tx/0xd82fe84e63b1aa52e1ce540582ee0895ba4a71ec5e7a632a3faa1aff3e763873), [0x86fe](https://etherscan.io/tx/0x86fe6933f03c14f75167e560e6635a23fab59b268e68c86d60c4d46afe0aeabc), [0x606a](https://etherscan.io/tx/0x606a49471ebcb608bf034abd982750d626b7eaf45a963118d5aae5baae4a2da2)

The vulnerability was in the [function](https://etherscan.io/address/0xf28a352377663ca134bd27b582b1a9a4dad7e534#code) `depositToGasZipERC20()` of `GasZipFacet` contract. The `GasZipFacet` contract was deployed by the LI.FI team five days prior to the attack to enable gas refueling for bridging transactions. Function `depositToGasZipERC20()` included a user-controlled argument `_swapData`, which was later passed to the function call `LibSwap.swap()`. Unfortunately, `LibSwap.swap` included a low-level call that can execute arbitrary functions with the call target and call data specified by the attacker-controlled argument `_swapData`. The attacker leveraged this "arbitrary call vulnerability" to  execute unauthorized transfers from users who had given infinite approval to the Li.Fi Diamond contract.

On July 16, 2024, the attacker initiated nearly one hundred [transactions](https://etherscan.io/tokentxns?a=0x8b3cb6bf982798fba233bca56749e22eec42dcf3&p=4) exploiting the arbitrary call vulnerability, transferring approximately \$ $11$ million worth of stable tokens (USDT, USDC, DAI) to Address [0x8b3c](https://etherscan.io/address/0x8b3cb6bf982798fba233bca56749e22eec42dcf3) within 30 minutes. Almost all siphoned stable tokens were then quickly swapped for the Ethereum native token ETH. The DEX utilized by the attacker included Uniswap, Metamask Swap, among others. Swap transaction examples: [0xdf9b](https://etherscan.io/tx/0xdf9b2b855e5a55f4add1b95f29f4c2be6917560c30e0de11ea55270eb711886e), [0x11d](https://etherscan.io/tx/0x11ddeef2d279de82717272301d6f6e08795a8ac83b90972c71761e015a9c644c), [0xb4a4](https://etherscan.io/tx/0xb4a4cb9084043b6fe2d789de51d6a15b6ce321cb6334ca033b5824562b2f5904). 

An example of the fund flow within a swap transaction [0x8e27](https://etherscan.io/tx/0x8e27acf21089de72cd927e2efc44d99af9494d56c21fd49eaf0c5b7e56e7316e) interacting with [Metamask Swap Spender](https://etherscan.io/address/0x74de5d4fcbf63e00296fd95d33236b9794016631). The attacker swapped the illegally acquired 333,258 USDT into 97.16 ETH.

![](misc/LIFI1.png)

Within two hours of the attack, all the stolen assets were transferred to downstream addresses controlled by the attacker and nothing is left in the original attack address. There are a total of 32 downstream addresses directly connected to Address [0x8b3c](https://etherscan.io/address/0x8b3cb6bf982798fba233bca56749e22eec42dcf3) (i.e., one hop away from the original attack address). Among these, 15 addresses received only 0.1 ETH from the attack address. As of October 22, 2024, the ETH held by these 15 addresses has not been transferred out. The remaining addresses have processed the rest of the large amounts of illicit funds.

Part of the fund flows from the victim addresses to the downstream addresses controlled by the attacker: 

![](misc/LIFI2.png)

After transferring the illicit funds to the downstream addresses one hop away from Address [0x8b3c](https://etherscan.io/address/0x8b3cb6bf982798fba233bca56749e22eec42dcf3), the attacker began further moving the funds in batches. The transfer (laundering) process lasted for nearly three months. Almost all of the illicit funds were finally moved to [Tornado Cash](https://etherscan.io/address/0xd90e2f925da726b50c4ed8d0fb90ad053324f31b) (99.9%), and a small portion were sent to the exchange [eXch](https://etherscan.io/address/0xf1da173228fcf015f43f3ea15abbb51f0d8f1123) for direct cash-out. There were totally 114 transactions that the attacker used to interact with the [Tornado Cash Router](https://etherscan.io/address/0xd90e2f925da726b50c4ed8d0fb90ad053324f31b). Examples of transactions moving illicit gains to Tornado Cash: [0x07de](https://etherscan.io/tx/0x07dec7b53ed73e310757b9cc4d73abac6726f867a68a4910082b59ce224a5a6e), [0xfe82](https://etherscan.io/tx/0xfe82ce08803579edd2ad69f7058cf52310b231890ec156ef5dcd046b5b296d03), [0x6a47](https://etherscan.io/tx/0x6a47684a769e4ac476506d245137839013e4526b437011d543be45886bc3713f), [0x8ea6](https://etherscan.io/tx/0x8ea6ca1047fd0a8cecafb38bb10a1029f82ef88932c1779de6b2a2ef5580d65e). Examples of transaction moving illicit gains to eXch: [0xaa89](https://etherscan.io/tx/0xaa89e7ab86da0f57640c6b40186f41d9a074c4f365cde6541965c1c908d7ded9), [0x7e65](https://etherscan.io/tx/0x7e656b657609910ed45590cc855738839e44b7e0589e9e5568991d8b5ae2c498), [0x8572](https://etherscan.io/tx/0x8572fa167f89cdc65a8cbe5526b04024d372bd9b2ec7f9f161fbf08c71dd7f33), [0x625c](https://etherscan.io/tx/0x625c6c15cad716f205ed2c3d67fc91b5893f234dacf43dffa8b899940f08917a), [0x2dd2](https://etherscan.io/tx/0x2dd2dfda6c8db4372b39bb902f806a741b451eb38ab9d9dd0161532e69465972), [0xda71](https://etherscan.io/tx/0xda715e9430bd82a40fb3b15caa7bbf4e51d00a11e6bf93f0c2c777a1689503ea).

A part of the fund flows from layer2 addresses (2 hops away from the original attack address [0x8b3c](https://etherscan.io/address/0x8b3cb6bf982798fba233bca56749e22eec42dcf3)) to layer4 addresses:

![](misc/LIFI5.png)

The first large-scale batch of transfers occurred within the first week following the attack, between July 16 and July 22. The attacker transferred approximately \$ $500\mathrm{k}$ worth of illicit assets from Address [0x6a6d](https://etherscan.io/address/0x6a6df7cf485fdc6e6f7d4a8b818e1eacc31e664e) to Tornado Cash. The attacker's transfer of illicit funds exhibited distinct characteristics: they moved the funds to downstream addresses that were far away from the attack address (high-risk address), gradually funneling a portion to Tornado Cash. In the first batch, the longest transfer path reached up to *20 hops*. The attacker utilized extremely deep laundering path to obscure the illicit money flows. Between August and October, the remaining illicit funds were gradually transferred to Tornado Cash in transfer batches with the same characteristics.

An example of a transfer batch moving funds from Address [0x8e85](https://etherscan.io/address/0x8e85eace2fa757c1d97c5ebfb8b0622e5f23c5a1) (one hop from [0x8b3c](https://etherscan.io/address/0x8b3cb6bf982798fba233bca56749e22eec42dcf3)) to the Tornado Cash Router: 

![](misc/LIFI0.png)

As the figure illustrates, between August 13 and August 16, 2024, the attacker gradually transferred 206 ETH to Tornado Cash over a 12-hop path. At Address [0xe9f7](https://etherscan.io/address/0xe9f7cd0c410257850324d86536bd165cf4306e80), the attacker split 204 ETH into two transactions: 100 ETH was sent to Tornado Cash, while 104 ETH was forwarded to further laundering addresses. This splitting pattern was consistent throughout the entire transfer process. That is, the attacker used a new, deeper address with each interaction involving Tornado Cash.

Two days after the attack, LI.FI officially released an [incident report,](https://li.fi/knowledge-hub/incident-report-16th-july/) claiming they had successfully disabled the vulnerable contract facet across all chains and prevented any further unauthorized access. LI.FI initiated a compensation plan and fully reimbursed the affected users.  For the recovery of siphoned assets, they claimed that they would continue to engage with law enforcement authorities and relevant third parties, including security teams from the industry, to trace and attempt to recover funds drained. As of October 22, 2024, nearly all of the illicit funds have been transferred to Tornado Cash and Li.Fi has yet to release tracing reports.

Some Relevant Addresses and Transactions are listed below.

|                 Addresses                  |                         Transactions                         |   Illicit Money Flows    |
| :----------------------------------------: | :----------------------------------------------------------: | :----------------------: |
| 0x8e85eace2fa757c1d97c5ebfb8b0622e5f23c5a1 | [0xe237](https://etherscan.io/tx/0xe2379835bfd6985949e741562d23e1e3fae892546d7a7d68c3812e1319f26415), [0x0d23](https://etherscan.io/tx/0x0d23ba0e8b53787893d7ba8bf0de75fafb2b81d13377ae19b22ff8ab812d0608) |        206.49 ETH        |
| 0xcb7c341dc6172b642dcf4a14015be70a27e5b31e | [0x050c](https://etherscan.io/tx/0x050c935a0fb442aa5b4b669c7fa84bbf88bf3f6a43c50eb176d472cd4ac1d3d9), [0x37d4](https://etherscan.io/tx/0x37d411e434f6a5124a5e615f767dcbfc668d68355f8baac2ec5fd7ff4050ee94) | 873,568 USDT + 36.48 ETH |
| 0x7b93fa16c04cdcf91949d4f5f893f740992ae57e | [0x57ea](https://etherscan.io/tx/0x57eaa1a8acf151230f19c38a1c8470d93baf7341e45ddcb20cf7d586332cf992), [0x52ac](0x52acb20f7c2a235698f4a6238005d90d1364be6b62aa0f1595406bc07ab20260) |        332.02 ETH        |
| 0x3462d2523cded523ad47c14111aa1dcbe7773675 | [0xc66d](https://etherscan.io/tx/0xc66d0bb572f8d7589c029f73d1c317d087bb29bfb9d72bebce2baf695620655f), [0xc0ff](https://etherscan.io/tx/0xc0ffe4343e7adad323fa5169e375148828818c070f46da9cd00080e16cc024b4) |        120.55 ETH        |
| 0xd0be9c4c84068a9964c3781f540f703c300db268 | [0x0c3b](https://etherscan.io/tx/0x0c3b0192ec5730a9b608c965f7e5c2361e03704a762adcea18204c26262a2ef3), [0x1670](https://etherscan.io/tx/0x1670fbf41c53fc0e4382b98555fb7e9d1e1b519608b999581b0485755d6d712f) |        275.38 ETH        |

------

On August 20, 2024, a phishing transaction profited more than 54M stable token DAI. The drained address is a [vault](https://etherscan.io/address/0x2129f8a9b6c3092a600da82ce859b7a9a69983e4) funded by [Gemini](https://www.gemini.com/apac/hong-kong), and the associated "[Maker Vault Owner](https://etherscan.io/accounts/label/maker-vault-owner)" address is [0xf2b8](https://etherscan.io/address/0xf2b889437f243396b29e829908b5d8ebe2e13048).  The phisher lured the victim (the original owner of the vault) into signing a [transaction](https://etherscan.io/tx/0xb721c8d603d5cbac826d804b04fb4662952afe91af15cf2aa603d002d3410b87) to change the vault owner to an addresses controlled by the phisher and then executes a [transaction](https://app.blocksec.com/explorer/tx/eth/0xf70042bf3ae7c22f0680f8afa078c38989ed475dfbe5c8d8f30a50d4d2f45dc4) to drain the vault.

![](misc/54M0.jpg)

- Phishing Address: [0x0000db5c8b030ae20308ac975898e09741e70000](https://etherscan.io/address/0x0000db5c8b030ae20308ac975898e09741e70000)
- Address Draining the Vault: [0x5D4b2A02c59197eB2cAe95A6Df9fE27af60459d4](https://etherscan.io/address/0x5d4b2a02c59197eb2cae95a6df9fe27af60459d4)
- Transactions changing the Owner: [0x2805](https://etherscan.io/tx/0x28054acca764c58157e1e5779e5e6d1c9c858a7508b189655d370a82e2a0e07b), [0xb721](https://etherscan.io/tx/0xb721c8d603d5cbac826d804b04fb4662952afe91af15cf2aa603d002d3410b87)
- Draining Transaction: [0xf700](https://etherscan.io/tx/0xf70042bf3ae7c22f0680f8afa078c38989ed475dfbe5c8d8f30a50d4d2f45dc4)

On August 20, 2024, the [original owner](https://etherscan.io/address/0xf2b889437f243396b29e829908b5d8ebe2e13048) of the victim vault was tricked to sign a transaction that change the vault owner to a [address](https://etherscan.io/address/0x0000db5c8b030ae20308ac975898e09741e70000) controlled by the phisher. About five hours later, the phisher send a transaction to further change the owner to a new [address](https://etherscan.io/address/0x5d4b2a02c59197eb2cae95a6df9fe27af60459d4). 20 minutes after the new address getting full control on the vault, it signed a [transaction](https://etherscan.io/tx/0xf70042bf3ae7c22f0680f8afa078c38989ed475dfbe5c8d8f30a50d4d2f45dc4) that had 55M DAI siphoned from the vault. 

Then, within two hours, all illegally acquired DAI tokens are transferred to downstream addresses controlled by the phisher and nothing is left in the initial address draining the vault. There are a total of six downstream addresses directly connected to Address [0x5D4b](https://etherscan.io/address/0x5d4b2a02c59197eb2cae95a6df9fe27af60459d4) (i.e., one hop away from the initial address). The majority of DAI tokens (44M) are directly transferred to downstream addresses, while 10M are swapped for the native token (3880) ETH and then moved to address [0x8cc5](https://etherscan.io/address/0x8cc568f3565a7ed44b3b0af8765a7ef67b8bc2dc). The DEX used for the swap is [CoW Protocol: GPv2Settlement](https://etherscan.io/address/0x9008d19f58aabd9ed0d60971565aa8510560ab41). The swap transaction: [0x7c63](https://etherscan.io/tx/0x7c6305d30fbc9f9a94749750967ac54d7fced0d3bea5ea4d370d92514ed07802).

The fund flow graph for the siphoned DAI from the original address [0x5D4b](https://etherscan.io/address/0x5d4b2a02c59197eb2cae95a6df9fe27af60459d4) to the 1-hop downstream addresses.

![](misc/54M1.png)

After transferring the illicit funds to the 1-hop downstream addresses, the attacker began further moving the funds to deeper addresses in batches. During the transfer process, the phisher gradually swapped the DAI held by downstream addresses for ETH. At downstream addresses 4 hops from the initial address, all the stolen DAI had already been swapped into ETH. These illicit assets, in the form of ETH, then flowed into centralized exchanges ([eXch](https://etherscan.io/address/0xf1da173228fcf015f43f3ea15abbb51f0d8f1123), [KuCoin](https://etherscan.io/address/0x45300136662dd4e58fc0df61e6290dffd992b785), [ChangeNOW](https://etherscan.io/address/0x077d360f11d220e4d5d831430c81c26c9be7c4a4)) and cross-chain bridges ([THORChain](https://etherscan.io/address/0xd37bbe5744d730a1d98d8dc97c42f0ca46ad7146), [Hop Protocol](https://etherscan.io/address/0xb8901acb165ed027e32754e0ffe830802919727f)). (Click the name to explore these cash-out addresses.) Examples of transactions depositing illicit gains to eXch: [0x2e42](https://etherscan.io/tx/0x2e42521b5142068b650c93d9ac798cad0fffff5ae35662f77190a4c42807fa9b), [0xa982](https://etherscan.io/tx/0xa98216405a9e42511523aa89047ea917f0098fc15c3d1162d0d4994ff02795a1), [0x1e1e](https://etherscan.io/tx/0x1e1e72d2b3bcab3138a098eaee5a4e942b3f44b4dd911b1a975f3b2f3a320b84), [0xb7a9](https://etherscan.io/tx/0xb7a94448befb8f3ced5164c27395f071b2ad23d32847990d7b70323601ed4398). Examples of transactions moving illicit gains to THORChain: [0x5c06](https://etherscan.io/tx/0x5c060ac781d4d9b22098282109e03af942b69c86ef10f2be21a736ab8d9772c0), [0xf824](https://etherscan.io/tx/0xf824553170776aa3f877047e72b3badb27763127ff6f2b54006eac01ec2b815f), [0x391e](https://etherscan.io/tx/0x391ed30bb238483c47a80ddf78d41a3eabb1ae9c1d90386aa57bd85ee6982a3f).

A part of the fund flows from layer2 addresses (2 hops away from the initial address) to layer5 addresses:

![](misc/54M3.png)

Among the transfers of illicit gains to deep downstream addresses, the longest transfer path reached up to 12 hops, where about 80k dollars were moved to the exchange [KuCoin 17](https://etherscan.io/address/0x45300136662dd4e58fc0df61e6290dffd992b785). As the below fund flow graph illustrates, between August 21 and August 22, 2024, the attacker gradually transferred 38 ETH to the centralized exchange over a 12-hop path.

![](misc/54M2.png)

To avoid drawing excessive attention from large transfer amounts, the perpetrators tend to split large funds across multiple addresses and use smaller transfers to move the assets to deeper addresses. An example of splitting 1.65M DAI into 36 small pieces, processed by a 1-hop address [0x860c](https://etherscan.io/address/0x860cf33bdc076f42edbc66c6fec30aa9ee99f073): 

![](misc/54M4.png)

Some Relevant Addresses and Transactions are listed below.

|                 Addresses                  |                         Transactions                         |   Illicit Money Flows    |
| :----------------------------------------: | :----------------------------------------------------------: | :----------------------: |
| 0x860cf33bdc076f42edbc66c6fec30aa9ee99f073 | [0xa11e](https://etherscan.io/tx/0xa11ebe4db723c16b275efd2cbb2d07f53c3d1931f3220d0cb8835ef869aa9ee0), [0x9ef1](https://etherscan.io/tx/0x9ef1b32d09461d83882c6acf4d8b710af00287ff4d079cafa913a62aac55982c) |      1,650,000 DAI       |
| 0xdd6397104d57533e507bd571ac88c4b24852bce9 | [0x7af2](https://etherscan.io/tx/0x7af275b3242b57a50a2ece06b9114ab4497e08b927cce50c51417c7f702e9abb), [0x1d45](https://etherscan.io/tx/0x1d4532bec533c0639c0f93a9cbf95a8a86e51d9f172c7fa911e83e88f9ab8078) |      36,733,858 DAI      |
| 0x8cc568f3565a7ed44b3b0af8765a7ef67b8bc2dc | [0x7e10](https://etherscan.io/tx/0x7e101dc35d6acf5068551bef00b08ff666773af3b14b46a85e4d41602718d05b), [0x5d08](https://etherscan.io/tx/0x5d0813cf16dfedcd9b1acfc0c7e4faad2607b47e293ec8c65725fcead2f5213f) | 3879 ETH + 1,825,000 DAI |
| 0xca6061c6e5a7c3657297f9cc45ce110dc4d14470 | [0xee0d](https://etherscan.io/tx/0xee0d2daaf6d79721f34ff3b2e8bdeb1fa61e10ef9a15570fa0cfa6f65fc85252) |         875 ETH          |
| 0x77b9f7e33e42b1f64b915bb27e9a9bca199da83e | [0xf97a](https://etherscan.io/tx/0xf97a281f71a89503f031ffab006456dbd880932bfa8e20ba39784e1f98ffa17a), [0xbc5c](https://etherscan.io/tx/0xbc5cb9114852ca36f0c4f0dc437779d2d34b17fa4d698ccb9bc581d78475f851) |         2164 ETH         |

