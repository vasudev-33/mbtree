package edu.sunysb.dbManager;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class MBTCreator extends Thread {
	private final int NUM_RETRY = 10;
	private final boolean UDBG = false;
	public int branchingFactor=25;
	public int height=4; 
	public static String rootHash="fe9280af7e6e06040f718a11085d1b507127559c";//"99e5b1c82c132b980c72bd9fe1ee180b0859327b";//"fc71b32e457746969cd784f33d57e01ef3e9dcd0";
	public int valueSize=50;
	public char charToFill='c';
	public int numKeys;
	public int threadId;
	public int numRuns;
	public int range;
	public int threadLow;
	public int threadHigh;
	public int runType;
	public String newValueToUpdate;
	public int threadStartIndex;
	//public static final int numRuns=10000;
	BufferedWriter bw;
	CallableStatement stmt=null;
	Statement rootHashStmt=null;
	Connection connection=null;
	int totalThreads;
	public int invalidCount;
	public static int congestionCount;
	//int numTimes=4;
	public long randomGenerationTime=0;
	public static float avgUpdatesPerSecond;
	public boolean RAND_GEN=true;
	public int[] leftElements={ 62134044,
72866575,72979464,72874059,10180961,72923644,72979549,64989640,72969970,72978983,72724863,72792839,70690369,64181983,72979231,71934323,72890372,72979649,72979658,72921616,72979168,72978937,72542992,9370367,71992359,72958964,72921123,72979870,72978988,71761863,72979785,72642467,72109926,70457814,72979445,66289848,71049010,72979883,72979887,72681166,70517724,63677901,72975571,72979696,50256343,72977333,72968224,72964883,72962355,72979508,13711773,72977878,72978970,72213932,72979551,72974331,72978162,69074123,71350007,72969514,72979796,23771487,72970929,72963529,72673430,72976200,71584159,58978155,7172416,53948311,72624079,70835891,72978930,72979899,72697194,72883110,72638059,70181599,30477078,72593426,72979909,72966698,37778809,68009863,72979048,72979829,69512289,71033449,72888669,72979816,72979891,72978114,71744534,72888025,68738835,72963431,72979427,72362194,72958726,72710961,72979815,72970234,72389625,16313975,72974793,72979904,72979552,71682512,72976497,50764666,72979937,72000826,72681633,53828086,72979442,65234327,72443462,42214141,70381326,72927835,72979974,72979865,72975637,72979285,72752477,72176213,53827181,72976330,72921927,71582649,71391409,72975996,72829732,72979853,72950884,72979717,72976543,72978016,72978964,68131349,67565771,72862753,72977041,72976832,72977125,72979728,72979096,72911038,69129347,15594022,69776653,72979824,64387124,72979907,72898752,72977885,72979996,72979804,72469944,59965300,72979961,45083136,72978290,72568853,72978644,72387076,72979804,72931801,72921582,72327070,72974805,36169540,72975025,72976051,72969153,72978028,12852156,72973126,72903215,19914886,72979579,72726653,72837570,72952346,72979954,70482776,3354627,72979823,72757188,72885692,72979942,72978668,72958960,72979941,71882252,17360296,72979399,72979901,49412598,72974963,65342572,69996423,72846095,72975951,72726512,72978348,72728341,72348996,6785457,72956344,72977824,55678438,72890807,72974110,72046781,65757612,72979985,57476011,72977640,72960516,72979926,70904875,34868519,72971850,72950384,72780588,58738426,68368788,72531743,72251262,47171061,72976644,72978735,72977249,72975949,72881270,67365518,72900478,72974715,72958851,72723910,68342018,72965705,72968610,72940421,63472424,72976126,72979587,72979251,72979800,72938970,72962451,72979910,72977940,72976851,72780136,59315066,43407891,49323240,71752113,67878096,45729109,54345204,72965097,72979659,72979853,72958687,72979492,71980723,72979393,72914116,57217770,72977879,72979896,72392084,72953431,72979932,71890458,69278097,72979731,72975036,72979751,72899540,72979895,72860916,72970457,72795486,72979899,57555597,67013756,72979850,66189774,72979413,69551331,65414187,72979810,72979851,72975025,72979915,72976488,72978112,72976675,72563149,72946052,72967939,72979924,72979707,71087473,72979584,72900023,72974742,72249405,72972075,72050564,14877613,72979988,72979227,72975754,72509569,72926476,56540439,72979581,72979841,72979662,68825704,72968542,72979997,71978102,72888853,72825092,72979492,72968780,60406969,72979891,72970672,72909214,72936542,72975404,72943611,72874816,72976020,72979323,69128909,71931816,3715564,72973589,72819488,72979960,20322307,72813776,48010523,72979770,72979840,72938730,72855386,72774217,72979859,72853892,68533953,72967083,71477207,72829655,57828411,72979695,70839856,72701562,72979475,72965488,72979586,66124689,72967116,72396752,72979939,72969124,72979975,72975498,72973757,71415878,72979694,72935026,43173852,72979015,72647248,68066391,72344221,11047216,72110550,72976368,72976232,72949704,72978483,72940494,72979953,67822738,67983419,72977285,71293763,71310228,72979394,72979996,72830994,34178977,72979974,72979962,72979953,3457932,72979910,72978263,40909182,47070349,72979141,72974994,21059964,65593906,72979848,44669537,68489430,72808155,72890656,72979590,72885178,72979488,72687442,71196522,72978868,72979838,72978555,72979957,72974088,72918003,72979663,72978611,72738349,72893840,72763677,71443312,72979418,72447597,72798047,72922861,72964351,72976568,72977496,56238757,72698450,67408413,72928809,72979739,31086300,69784854,72967726,72979932,71475116,72329528,72977327,72942115,72591125,72975446,67736916,72978923,72974760,72978963,72938871,72978688,72977642,72979902,72979215,72964856,72979556,72979924,72979935,53595875,72898150,72861411,72979631,32031302,71669352,37181641,72926454,35417375,72914761,72520020,72397647,72603141,41064621,72887393,70172904,72891479,69946128,72746495,72978090,72974898,72979713,33168199,72979921,72978605,72778917,72979765,72751462,10032036,72801850,54249480,10788767,72979894,72962249,71072190,63016773,72951034,72979905,72979393,72979260,72979744,72979325,72821170,72978679,72946166,63806813,72510694,72929045,72979337,72979983,72979801,72617483,72954850,9985496,30384156,72925462,72978393,72979980,72937642,72979925,72974825,72928907,65858394,72973147,71136574,72979877,69395538,72977038,72979161,72964027,69859114,72979693,72972917,56850472,72979874,72975263,25723903,72460543,64023872,72697528,71860987,72979941,72979995,72903957,61355908,72144989,72306065,64380715,72886052,72979834,68401114,72967732,72977077,72979787,71528104,72887038,72972292,68137033,62172883,72979507,41193128,71457314,72979979,72965168,72979953,69796880,72944210,72979880,48409827,72979813,72976742,72915943,72977887,72948278,27347614,72979921,72979506,72918229,72574009,72979996,72906733,55868088,46973849,72459486,72979355,72960053,72979793,72651395,72979351,72979599,72850281,5861978,70991279,72971666,71094178,72979997,72941732,72979913,62323583,72979875,38960324,72849567,72976296,72979852,72979725,61283059,72972919,72460434,72942368,72979942,72731663,72979706,71954809,72396518,68940158,71836423,72755136,72825012,72979898,72977483,72475693,8832166,72977843,68331344,72722556,72977964,72978208,72972953,68832796,72978685,66926722,63712701,72978632,72740481,71637708,72118679,72976596,72259860,55525786,72979909,66726252,72978488,13903155,72979888,64929116,46925332,72634567,72978998,46298588,72979517,72916705,71351770,72978864,72942149,72974822,51854484,72977902,58250546,72968496,70762679,72979839,72979925,72898617,46989401,72977200,58049917,72949103,72369277,69121057,72979459,72979030,72248512,72979640,72731840,72979944,72979840,62764451,72979761,72858223,60196334,72937417,72634782,72975678,72972493,72940145,72276287,66579459,72979847,72959743,72979272,72979870,72971824,1919673,72499666,64838354,72972448,72956815,72973153,65329347,72979327,50420679,71844805,72978674,71949875,69077433,72979890,52665491,72976609,72979817,69838990,53375918,72662827,71338679,72979959,72978048,72979673,13376217,72976068,66430286,72945761,71294092,72759219,72973680,72970694,72967118,72978551,72977984,8085748,72979806,72870356,72968129,72667599,72979804,72830616,72979832,54996105,72182436,62516661,72417068,72969196,70003654,65390040,71013072,67608759,72976436,72972287,72972054,72753540,4182283,62996823,68448355,72639053,72727689,72978390,72973688,72978731,51762158,70089926,48697152,72978926,72869581,38930657,72971356,72979737,72979308,72556868,72973176,72911197,72972653,65229006,72907289,72975632,72465970,29492857,26948897,72978802,72979676,72979570,72692402,72970541,72977465,70042982,72979463,72946608,72977614,70129839,72709118,72976806,72979896,72885098,72744923,72964351,72978987,63075886,72979870,72769292,63237208,72974772,72978796,65404790,71521398,72979561,28770840,72979383,72836978,72971371,45040502,25569233,8266150,72948376,72942695,72976452,72979972,72979656,71624511,72903979,70863333,72854891,72861972,72336965,72939828,65967966,64305962,72979636,72928443,72979786,63957734,72978423,72652699,72957286,72968510,72536228,30189899,64739781,72956706,72973596,72792392,72715384,72979273,48922834,72979940,72957238,72875770,58264286,72978532,71436648,72970170,72049206,71674462,72117924,72889848,71670960,71425457,35375226,72979888,72979960,72940412,70250637,72977318,72935740,72978754,72975401,72963605,72979724,72546779,72979497,72907734,72979032,71871104,72973433,72914850,72975101,71441266,72297954,68433825,72979918,71600705,72979988,72978663,65769272,72979983,72971630,42070779,72950964,72976663,72923755,72979457,72831653,68544953,72691613,71425866,71691120,68602745,14408750,72729337,54497842,72932451,72444308,69051801,49290104,72959354,72926935,71155060,72970078,72979719,72858683,16819574,41541072,72035350,71391873,49132557,72596124,72946191,72979837,72979932,31207892,72612289,71557321,72979959,72969445,72970714,71266335,72979801,63108427,36919065,72962301,71586443,42811519,72948175,60917197,72979946,26393996,72765606,66391571,72979981,72979999,72973720,72979204,67775191,72932114,72978459,72964317,72978839,64815665,72449109,59929566,72977854,72976869,72979811,69504100,72707570,72979899,71784671,72968442,72979919,57998936,72979788,72979960,72851285,72977937,72800878,71993447,72979701,72596448,72979712,52630959,72899820,72979771,72809859,72979766,72373238,69039404,48490442,52777681,72904510,72978295,72978649,72977878,71071400,72978627,72325321,72857474,60833434,72979925,72979919,72702331,72974943,72912770,72879696,72956586,49770368,72888586,29809761,70633628,72968147,72979729,72967670,72936570,72893668,72973064,42062320,72979649,3631581,25444857,45400160,72854895,72979950,72788569,72975018,72803522,72569250,71427467,72978799};
	
	public int[] rightElements={
62134074,72866616,72979509,72874136,10181034,72923715,72979644,64989687,72970006,72979022,72724864,72792866,70690445,64181994,72979320,71934337,72890441,72979654,72979693,72921660,72979200,72978992,72543089,9370389,71992453,72959011,72921143,72979883,72979002,71761898,72979809,72642502,72109993,70457852,72979512,66289877,71049084,72979929,72979932,72681179,70517784,63678001,72975641,72979745,50256371,72977379,72968239,72964963,72962443,72979561,13711809,72977970,72979052,72214020,72979605,72974332,72978189,69074167,71350070,72969592,72979874,23771495,72971025,72963625,72673460,72976202,71584237,58978165,7172505,53948375,72624165,70835903,72978986,72979969,72697225,72883210,72638153,70181643,30477114,72593516,72979953,72966740,37778813,68009927,72979068,72979904,69512330,71033535,72888675,72979826,72979961,72978194,71744592,72888055,68738888,72963522,72979441,72362205,72958767,72711006,72979900,72970301,72389716,16314058,72974812,72979964,72979643,71682563,72976583,50764717,72979938,72000899,72681679,53828153,72979458,65234331,72443527,42214240,70381357,72927859,72979975,72979961,72975683,72979338,72752485,72176244,53827215,72976397,72922009,71582733,71391500,72976084,72829736,72979888,72950905,72979768,72976610,72978053,72978968,68131446,67565778,72862767,72977139,72976870,72977173,72979804,72979189,72911125,69129391,15594083,69776674,72979844,64387165,72979938,72898817,72977919,72979997,72979829,72469950,59965318,72979982,45083170,72978388,72568904,72978645,72387146,72979854,72931871,72921671,72327072,72974854,36169590,72975103,72976067,72969188,72978127,12852248,72973138,72903224,19914889,72979638,72726677,72837655,72952425,72979998,70482874,3354670,72979902,72757232,72885732,72979958,72978740,72959025,72979991,71882255,17360334,72979426,72979994,49412656,72975031,65342643,69996480,72846100,72975966,72726586,72978385,72728407,72349008,6785536,72956431,72977880,55678538,72890827,72974113,72046795,65757626,72979986,57476043,72977713,72960604,72979955,70904966,34868529,72971909,72950429,72780662,58738485,68368800,72531755,72251322,47171085,72976696,72978824,72977325,72975997,72881341,67365532,72900549,72974808,72958858,72723960,68342114,72965717,72968669,72940457,63472463,72976151,72979625,72979310,72979887,72939028,72962482,72979983,72978001,72976876,72780224,59315094,43407897,49323327,71752200,67878182,45729163,54345271,72965179,72979701,72979879,72958760,72979586,71980790,72979485,72914210,57217834,72977943,72979902,72392108,72953459,72979977,71890463,69278144,72979778,72975067,72979829,72899608,72979909,72860996,72970513,72795511,72979952,57555685,67013827,72979894,66189820,72979498,69551398,65414273,72979860,72979904,72975103,72979965,72976514,72978197,72976688,72563247,72946108,72967953,72980000,72979728,71087555,72979607,72900075,72974833,72249484,72972105,72050648,14877708,72979994,72979254,72975833,72509634,72926564,56540531,72979675,72979895,72979715,68825771,72968551,72980000,71978163,72888888,72825164,72979548,72968861,60407023,72979986,72970676,72909280,72936631,72975467,72943669,72874913,72976079,72979416,69128998,71931842,3715639,72973634,72819497,72979998,20322364,72813793,48010614,72979809,72979891,72938784,72855429,72774233,72979863,72853990,68533997,72967153,71477240,72829730,57828426,72979729,70839932,72701577,72979479,72965576,72979604,66124760,72967157,72396804,72979990,72969128,72979991,72975557,72973781,71415897,72979793,72935028,43173891,72979019,72647309,68066435,72344269,11047217,72110594,72976463,72976276,72949801,72978578,72940587,72979976,67822783,67983476,72977358,71293836,71310285,72979467,72979997,72831093,34179023,72979978,72979976,72979959,3458017,72979950,72978271,40909274,47070411,72979198,72975066,21059968,65593948,72979916,44669612,68489473,72808176,72890719,72979655,72885231,72979497,72687465,71196527,72978870,72979873,72978600,72979983,72974138,72918099,72979757,72978662,72738381,72893850,72763734,71443384,72979467,72447640,72798117,72922954,72964400,72976661,72977501,56238793,72698483,67408494,72928903,72979785,31086382,69784895,72967806,72979966,71475177,72329537,72977375,72942127,72591186,72975473,67736960,72978970,72974793,72979006,72938907,72978697,72977720,72979909,72979310,72964948,72979592,72979925,72979964,53595954,72898229,72861429,72979637,32031382,71669447,37181714,72926469,35417398,72914788,72520117,72397692,72603195,41064651,72887399,70172979,72891536,69946141,72746505,72978190,72974971,72979765,33168269,72979936,72978681,72778926,72979767,72751509,10032125,72801934,54249531,10788862,72979977,72962319,71072191,63016856,72951058,72980000,72979424,72979339,72979771,72979332,72821178,72978775,72946214,63806909,72510768,72929120,72979368,72979987,72979873,72617511,72954948,9985588,30384162,72925545,72978489,72979996,72937723,72979968,72974862,72928950,65858489,72973158,71136641,72979936,69395577,72977123,72979180,72964103,69859177,72979718,72972920,56850515,72979923,72975352,25723932,72460579,64023936,72697591,71861057,72979992,72979998,72904032,61355989,72145010,72306080,64380809,72886103,72979864,68401171,72967811,72977173,72979803,71528141,72887100,72972300,68137088,62172924,72979527,41193137,71457408,72979984,72965181,72979965,69796899,72944227,72979928,48409830,72979827,72976811,72915963,72977919,72948353,27347649,72979933,72979508,72918232,72574081,72979998,72906792,55868115,46973941,72459558,72979416,72960135,72979859,72651491,72979357,72979610,72850334,5861992,70991372,72971669,71094235,72980000,72941790,72979968,62323589,72979887,38960348,72849600,72976322,72979875,72979773,61283150,72973013,72460519,72942399,72979978,72731740,72979769,71954842,72396528,68940173,71836453,72755144,72825017,72979911,72977520,72475730,8832256,72977885,68331368,72722643,72978039,72978299,72973033,68832881,72978745,66926774,63712773,72978723,72740506,71637719,72118694,72976646,72259940,55525811,72979962,66726350,72978497,13903237,72979967,64929160,46925374,72634592,72979062,46298685,72979616,72916799,71351856,72978919,72942195,72974839,51854549,72977911,58250573,72968591,70762683,72979878,72979987,72898677,46989466,72977258,58049980,72949182,72369305,69121100,72979547,72979085,72248587,72979685,72731903,72979972,72979933,62764508,72979837,72858292,60196429,72937513,72634850,72975770,72972539,72940196,72276331,66579555,72979945,72959800,72979277,72979936,72971826,1919773,72499734,64838419,72972525,72956860,72973235,65329372,72979366,50420759,71844847,72978710,71949896,69077526,72979933,52665561,72976706,72979871,69839024,53376011,72662879,71338728,72979969,72978099,72979722,13376299,72976089,66430288,72945764,71294147,72759307,72973775,72970790,72967146,72978578,72978010,8085789,72979837,72870423,72968149,72667631,72979859,72830711,72979844,54996112,72182515,62516714,72417112,72969287,70003660,65390140,71013109,67608843,72976525,72972335,72972085,72753633,4182333,62996847,68448429,72639141,72727777,72978436,72973777,72978800,51762171,70089956,48697221,72979019,72869594,38930751,72971377,72979796,72979340,72556923,72973256,72911271,72972680,65229007,72907364,72975713,72466011,29492889,26948958,72978892,72979737,72979640,72692463,72970631,72977506,70043007,72979481,72946628,72977637,70129875,72709153,72976871,72979923,72885182,72744955,72964406,72978994,63075948,72979918,72769391,63237291,72974776,72978800,65404815,71521447,72979564,28770937,72979460,72837006,72971421,45040520,25569325,8266241,72948444,72942766,72976470,72979978,72979716,71624522,72903988,70863364,72854969,72862063,72337018,72939871,65968003,64305971,72979659,72928479,72979882,63957770,72978459,72652734,72957365,72968557,72536236,30189911,64739786,72956769,72973692,72792444,72715426,72979292,48922932,72979954,72957317,72875868,58264316,72978571,71436743,72970180,72049207,71674530,72117941,72889856,71671026,71425469,35375236,72979974,72979993,72940486,70250685,72977400,72935764,72978795,72975409,72963672,72979789,72546788,72979570,72907772,72979124,71871125,72973447,72914930,72975158,71441311,72298023,68433924,72979950,71600708,72979989,72978692,65769372,72979988,72971657,42070814,72951063,72976730,72923820,72979496,72831686,68545036,72691707,71425876,71691153,68602831,14408846,72729424,54497884,72932531,72444323,69051828,49290108,72959379,72926966,71155111,72970143,72979743,72858725,16819642,41541156,72035400,71391908,49132648,72596178,72946235,72979869,72979991,31207907,72612300,71557335,72979991,72969502,72970760,71266336,72979833,63108429,36919111,72962391,71586534,42811520,72948273,60917283,72979998,26394031,72765685,66391636,72979985,72980000,72973736,72979217,67775252,72932126,72978486,72964328,72978904,64815666,72449195,59929580,72977874,72976896,72979905,69504152,72707651,72979945,71784729,72968529,72979942,57998991,72979809,72979984,72851374,72977941,72800978,71993500,72979735,72596514,72979714,52630973,72899852,72979815,72809921,72979788,72373246,69039405,48490537,52777694,72904525,72978347,72978652,72977973,71071446,72978694,72325389,72857534,60833437,72979942,72979926,72702341,72974965,72912806,72879781,72956655,49770394,72888630,29809823,70633714,72968209,72979806,72967752,72936619,72893722,72973137,42062334,72979650,3631607,25444921,45400242,72854939,72979960,72788638,72975105,72803530,72569285,71427534,72978802
	};
	
	/*public static void main(String args[]) throws IOException{
		MBTCreator mbtCreator= new MBTCreator();
		int numLeaves=(int) Math.pow(mbtCreator.branchingFactor,mbtCreator.height);
		mbtCreator.numKeys=numLeaves*(mbtCreator.branchingFactor-1);
		System.out.println("Number of leaves="+numLeaves);
		System.out.println("Number of keys="+mbtCreator.numKeys);
		
//		try {
//			File file = new File("btree.txt");
//			FileWriter fw = new FileWriter(file.getAbsoluteFile());
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		//long startTime=System.currentTimeMillis();
		//BTreeManager bTreeManager=new BTreeManager(dbManager,numLeaves*(mbtCreator.branchingFactor-1));
		//bTreeManager.emptyDataTable();
		//String fixedStringVal=bTreeManager.fillString(valueSize, charToFill);
		//bTreeManager.populateBTreeLeaves(mbtCreator.height,mbtCreator.branchingFactor,fixedStringVal);
		//bTreeManager.populateBTreeInternalNodes(mbtCreator.height,mbtCreator.branchingFactor);
		//long endTime=System.currentTimeMillis();
		//long diff=TimeUnit.MILLISECONDS.toSeconds(endTime-startTime);
		//System.out.println("Time for Constructing BTree= "+diff+" seconds");
		
		
//		long startTime=System.currentTimeMillis();
//		MBTreeManager mbTreeManager=new MBTreeManager(dbManager,mbtCreator.branchingFactor,mbtCreator.height);
//		mbTreeManager.emptyMBTreeTable();
//		mbTreeManager.populateMBTreeLeaves(mbtCreator.height);
//		mbTreeManager.loadDataIntoTable(mbtCreator.height);
//		mbTreeManager.populateMBTreeInternalNodes(mbtCreator.height,mbtCreator.branchingFactor);	
//		dbManager.closeConnection();
//		long endTime=System.currentTimeMillis();
//		long diff=TimeUnit.MILLISECONDS.toSeconds(endTime-startTime);
//		System.out.println("Time for Constructing MBTree= "+diff+" seconds");
		int numThreads=0;
		for(int i=10000;i<=10000;i=i*10){
			int numIters=10000;
			mbtCreator.initRuns(++numThreads, numIters, i );
		}
		
	}*/
	
	public MBTCreator(int[] clonedLeft, int[] clonedRight) {
		leftElements=clonedLeft.clone();
		rightElements=clonedRight.clone();
		
		// TODO Auto-generated constructor stub
	}

	public MBTCreator() {
		// TODO Auto-generated constructor stub
	}

	public void run() {

		DBManager dbManager= new DBManager();
		FileWriter fw;
		try {
			if(UDBG){
				fw = new FileWriter("results"+threadId+".txt");
				bw=new BufferedWriter(fw);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			//DBManager.openConnection();
			connection=dbManager.openConnection();
			connection.setAutoCommit(false);
			String searchString = "{call search(?,?,?,?)}";
			stmt=connection.prepareCall(searchString);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long startTime=System.currentTimeMillis();
		
		try {
			performRunsForUpdate(threadId, numRuns, range, threadLow, threadHigh);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//mbtCreator.update(755363,756545,"ssss");
		//mbtCreator.update(755363,756545,"jjsssj");
		//mbtCreator.update(855363,855567,"hhhhh");
		dbManager.closeConnection();
		//test case for mbtree left boundary fix
		//mbtCreator.search(6120001,6161691);
		//test case for mbtree right boundary fix
		//mbtCreator.search(6120002,6135000);
		
		//test case for mbtree random left boundary fix
		//mbtCreator.search(47648849, 47668849);
		//test case for mbtreerandom right boundary fix
		//mbtCreator.search(47745926,47765926);
		
		long endTime=System.currentTimeMillis();
		long diff = endTime-startTime;
		//System.out.println("Total time for "+numRuns+" runs with range "+range+"= "+diff+" milliseconds");
		try {
			if(UDBG){
			bw.write("Total time for "+numRuns+" runs with range "+range+" = "+diff+ " milliseconds\n");
			bw.write("Total time for "+numRuns+" runs with range "+range+" = "+TimeUnit.MILLISECONDS.toSeconds(endTime-startTime)+" seconds\n");
			bw.write("Total time for "+numRuns+" runs with range "+range+" = "+TimeUnit.MILLISECONDS.toMinutes(endTime-startTime)+" minutes\n");
			bw.close();
			System.out.println("Thread "+threadId+" completed with "+numRuns+" runs. Time Taken= "+TimeUnit.MILLISECONDS.toSeconds(endTime-startTime)+" seconds. Invalid Count="+invalidCount);
			}
			
			//float numUpdatesPerSecond=((float)numRuns/(float)diff)*1000;
			
			//logSummaryResults(threadId, numRuns, range,diff, totalThreads,numUpdatesPerSecond);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	
		
	}
	
	public synchronized void logSummaryResults(int threadId, int numRuns, int range, long diff, int totalThreads, float numUpdatesPerSecond){
		
		avgUpdatesPerSecond+=numUpdatesPerSecond;
		BufferedWriter cbw;
		try {
			cbw = new BufferedWriter(new FileWriter("summary-"+range+"-"+totalThreads+".csv",true));
			cbw.write(threadId+","+range+","+numRuns+","+diff+","+numUpdatesPerSecond+"\n");
			cbw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/*
	public void performRunsForSearch(int numRuns, int range) throws IOException{
		if(UDBG){
		System.out.println(range);
		}
		//int leftKey[]={3757, 123656, 8934678, 3789321, 234121, 7780023 };
		//int rightKey[]={4969, 123755, 8935677, 3799320, 334120, 9780022};
		
		Random rand=new Random();
		int low=2;
		//int low=4;
		//int firstKeyHigh=72983739;
		//int secondKeyHigh=72983754;
		int firstKeyHigh=numKeys-2;
		int secondKeyHigh=numKeys-1;
		int leftKey;
		int rightKey;
		HashSet hashSet=new HashSet();
		for(int i=0;i<numRuns;i++){
			do{
				leftKey=randInt(low,firstKeyHigh);
				rightKey=randInt(leftKey+1,secondKeyHigh);
			}while((rightKey-leftKey)>range);//|| hashSet.contains(leftKey) || hashSet.contains(rightKey));
			hashSet.add(leftKey);
			hashSet.add(rightKey);
			Calendar cal = Calendar.getInstance();
			long startTime=System.currentTimeMillis();
			if(UDBG){
			System.out.println("Iteration "+i+": "+"call search("+leftKey+','+rightKey+','+branchingFactor+','+height+')');
			}
			bw.write("Iteration "+i+": "+"call search("+leftKey+','+rightKey+','+branchingFactor+','+height+')'+"\n");
			
			search(leftKey,rightKey);
			long endTime=System.currentTimeMillis();
			long diff=endTime-startTime;//TimeUnit.MILLISECONDS.toSeconds(endTime-startTime);
			
			//System.out.println("Time for Search + Reconstruction of Root Hash= "+diff+" milliseconds");
			//bw.write("Time for Search + Reconstruction of Root Hash= "+diff+" milliseconds\n");
			if(UDBG){
			System.out.println();
			}
			//bw.write("\n");
		}
	}*/
	
	public void performRunsForUpdate(int threadId, int numRuns, int range, int threadLow, int threadHigh) throws IOException{
		
		
		Random rand=new Random();
		//int low=2;
		//int firstKeyHigh=numKeys-2;
		//int secondKeyHigh=numKeys-1;
		
		//int low=4;
		//int firstKeyHigh=72983739;
		//int secondKeyHigh=72983754;
		
		int low=threadLow;
		int firstKeyHigh=threadHigh;
		int secondKeyHigh=threadHigh;
		
		
		int leftKey=-1;
		int rightKey=-1;
		HashSet hashSet=new HashSet();
		//for(int i=0;i<numRuns;i++){
		for(int i=threadStartIndex;i<threadStartIndex+numRuns;i++){
			long startTime=System.currentTimeMillis();
			do{
				if(!RAND_GEN){
					
					leftKey=leftElements[i];//randInt(low,firstKeyHigh);
					rightKey=rightElements[i];//randInt(leftKey+1,secondKeyHigh);
					
				}else{
					leftKey=randInt(low,firstKeyHigh);
					rightKey=randInt(leftKey+1,secondKeyHigh);
				}
				//if((rightKey-leftKey)>range || leftKey==-1 || rightKey==-1)
					//System.out.println("Regenerating "+leftKey+" "+rightKey);
			}while((rightKey-leftKey)>range || leftKey==-1 || rightKey==-1);//|| hashSet.contains(leftKey) || hashSet.contains(rightKey));
			//hashSet.add(leftKey);
			//hashSet.add(rightKey);
			//Calendar cal = Calendar.getInstance();
			//System.out.println(leftKey+" "+rightKey);
			if(UDBG){
			System.out.println("Iteration "+i+": "+"call btreeUpdate("+leftKey+','+rightKey+','+branchingFactor+','+height+')');
			bw.write("Iteration "+i+": "+"call btreeUpdate("+leftKey+','+rightKey+','+branchingFactor+','+height+')'+"\n");
			}
			long endTime=System.currentTimeMillis();
			long diff=endTime-startTime;
			randomGenerationTime+=diff;
			update(bw, threadId, leftKey, rightKey, newValueToUpdate);
			//update(bw, threadId, 4699133, 4699377, "bbbbbbbbb");
			try {
				//synchronized(this){
				connection.commit();
				//}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//TimeUnit.MILLISECONDS.toSeconds(endTime-startTime);
			
			//System.out.println("Time for Search + Reconstruction of Root Hash= "+diff+" milliseconds");
			//bw.write("Time for Search + Reconstruction of Root Hash= "+diff+" milliseconds\n");
			
			//System.out.println();
			//bw.write("\n");
		}
		//System.out.println("Invalid="+invalidCount);
		
	}
	
	
	public int randInt(int min, int max) {

	    // Usually this can be a field rather than a method variable
	    Random rand = new Random();

	    // nextInt is normally exclusive of the top value,
	    // so add 1 to make it inclusive
	    int randomNum=-1;
	    try{
	    randomNum = rand.nextInt((max - min) + 1) + min;
	    }catch(IllegalArgumentException e){
	    	//e.printStackTrace();
	    	if(UDBG){
	    	System.out.println(min+" "+max);
	    	}
	    	return randomNum;
	    }
	    return randomNum;
	}
	
	/*
	public void search(int leftKey,int rightKey) throws IOException{
		
		MBTreeSearch mbTreeSearch=new MBTreeSearch(branchingFactor,height);
		mbTreeSearch.search(connection, stmt, leftKey, rightKey);
		
		if(MBTCreator.rootHash!=null && MBTreeSearch.rootHash!=null){
			if(MBTCreator.rootHash.equals(MBTreeSearch.rootHash)){
				//System.out.println("Root Hashes Match");
				//bw.write("Root Hashes Match\n");
			}else{	
				System.out.println("Mismatching root hashes");
				bw.write("Mismatching root hashes\n");
				System.out.println("Obtained Root Hash="+ MBTreeSearch.rootHash);
				System.out.println("Actual Root Hash="+ MBTCreator.rootHash);		
			}
		}else{
			bw.write("One of the root hash is null\n");
			System.out.println("One of the root hash is null");
			System.out.println("Obtained root hash="+ MBTreeSearch.rootHash);
		}
		
		
		
	}*/
	
public void update(BufferedWriter bw, int threadId, int leftKey,int rightKey, String newVal) throws IOException{
		
		int count=0;
		int retVal;
		do{
			if(count==NUM_RETRY){
				synchronized(this) {
				MBTCreator.congestionCount++;
				}
				break;
			}
			if(UDBG){
			System.out.println("Trying for "+leftKey+" "+rightKey);
			}
			MBTreeUpdate mbTreeUpdate=new MBTreeUpdate(branchingFactor, height);
			try {
				rootHashStmt=connection.createStatement();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			retVal=mbTreeUpdate.update(bw, threadId, stmt, leftKey, rightKey, newVal, rootHashStmt, runType);
			if(retVal==-1)
				invalidCount++;
			count++;
			
		}while(retVal==-3);
	
		
	}

/* public static void main(String args[]) throws IOException, SQLException{
	MBTCreator mbtCreator=new MBTCreator();
	
	int leftKey=9613914;
	int rightKey=9616842;
	int threadId=1;
	FileWriter fw = new FileWriter("results");
	mbtCreator.bw=new BufferedWriter(fw);
	
	DBManager dbManager=new DBManager();
	mbtCreator.connection=dbManager.openConnection();
	//mbtCreator.connection=dbManager.getConnection();
	mbtCreator.connection.setAutoCommit(false);
	String searchString = "{call search(?,?,?,?)}";
	mbtCreator.stmt=mbtCreator.connection.prepareCall(searchString);
	int retVal=-3;
	do{
		MBTreeUpdate mbTreeUpdate=new MBTreeUpdate(mbtCreator.branchingFactor, mbtCreator.height);
		//retVal=mbTreeUpdate.update(mbtCreator.bw, 1, mbtCreator.stmt, 4890863,4891528, "bbbbbb");
	}while(true);
} */

}
