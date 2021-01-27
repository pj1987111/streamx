package cn.tongdun.streamx.tools;

/**
 *  \* Created with IntelliJ IDEA.
 *  \* User: hongyi.zhou
 *  \* Date: 2021-01-26
 *  \* Time: 10:53
 *  \* Description: 
 *  \
 */
public class CamelCaseUtils {
    private static final char SEPARATOR = '_';

    public static String toUnderlineName(String s) {
        if (s == null) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        boolean upperCase = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);

            boolean nextUpperCase = true;

            if (i < (s.length() - 1)) {
                nextUpperCase = Character.isUpperCase(s.charAt(i + 1));
            }

            if ((i >= 0) && Character.isUpperCase(c)) {
                if (!upperCase || !nextUpperCase) {
                    if (i > 0) sb.append(SEPARATOR);
                }
                upperCase = true;
            } else {
                upperCase = false;
            }

            sb.append(Character.toLowerCase(c));
        }

        return sb.toString();
    }

    public static String toCamelCase(String s) {
        if (s == null) {
            return null;
        }

        s = s.toLowerCase();

        StringBuilder sb = new StringBuilder(s.length());
        boolean upperCase = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);

            if (c == SEPARATOR) {
                upperCase = true;
            } else if (upperCase) {
                sb.append(Character.toUpperCase(c));
                upperCase = false;
            } else {
                sb.append(c);
            }
        }

        return sb.toString();
    }

    public static String toCapitalizeCamelCase(String s) {
        if (s == null) {
            return null;
        }
        s = toCamelCase(s);
        return s.substring(0, 1).toUpperCase() + s.substring(1);
    }

    public static void main(String[] args) {
//        System.out.println(CamelCaseUtils.toUnderlineName("ISOCertifiedStaff"));
//        System.out.println(CamelCaseUtils.toUnderlineName("CertifiedStaff"));
//        System.out.println(CamelCaseUtils.toUnderlineName("UserID"));
//        System.out.println(CamelCaseUtils.toCamelCase("iso_certified_staff"));
//        System.out.println(CamelCaseUtils.toCamelCase("certified_staff"));
//        System.out.println(CamelCaseUtils.toCamelCase("user_id"));

        String[] vals = new String[]{
            "c_ex_vehicle_type",
            "c_ex_card_status",
            "c_other_type",
            "n_other_id",
            "vc_marks",
            "d_toll",
            "d_unpaid",
            "d_forfeit",
            "d_agency_toll",
            "vc_agencys",
            "c_category",
            "c_pay_way",
            "n_bill_no",
            "d_svc_balance",
            "n_prn_times",
            "n_box_id",
            "n_zone_time",
            "n_card_lane_id",
            "n_card_serial_no",
            "n_card_op_code",
            "c_card_license",
            "c_ex_license",
            "d_card_toll",
            "d_card_agency_toll",
            "c_part_vehicle_type",
            "c_special_type",
            "vc_card_agencys",
            "d_card_forfeit",
            "n_temp",
            "c_temp",
            "c_cardno",
            "c_bankexamno",
            "c_posserialno",
            "d_weight",
            "d_over_weight",
            "c_ticket_type",
            "c_verifycode",
            "n_ex_psam_id",
            "n_en_psam_id",
            "c_lane_type",
            "c_cacu_way",
            "vc_tac",
            "vc_issue_code",
            "d_fee_length",
            "d_fare0",
            "d_fare1",
            "d_fare2",
            "vc_fix_marks",
            "vc_fix_marks_status",
            "n_ex_cpu_psamid",
            "n_vehicle_seats",
            "n_trade_time",
            "n_trade_speed",
            "n_batch_no",
            "n_temp1",
            "c_temp1",
            "n_temp2",
            "c_temp2",
            "c_make_ticket",
            "n_en_sys_date",
            "n_favouredpolicy_type",
            "n_mb_trade_date",
            "n_mb_trade_time",
            "vc_temp3",
            "vc_iden_license",
            "n_marks_mode",
            "d_bef_balance",
            "c_en_lane_type",
            "c_en_category",
            "n_station",
            "n_ex_serial_no",
            "n_ex_lane_id",
            "c_send_flag2",
            "c_send_flag3",
            "c_send_flag1",
            "vc_marks1",
            "n_axis",
            "hhy_dt",
            "c_send_flag4",
            "vc_axle_specialtype",
            "n_exaxlecount",
            "vc_cardver",
            "n_algorithmidentifier",
            "vc_stationhex",
            "vc_exhex",
            "vc_enhex",
            "d_enweight",
            "n_enaxlecount",
            "vc_enaxletype",
            "d_enpermittedweight",
            "n_enspecialtruck",
            "vc_markhex",
            "d_originaltotalfee",
            "d_originalpassfare",
            "d_originalagencyfare",
            "d_originalfare0",
            "d_originalfare1",
            "d_originalfare2",
            "vc_rateversion",
            "vc_obumac",
            "vc_obusn as vcObusn",
            "n_electricalpercent",
            "n_multiprovince",
            "vc_pass_id",
            "vc_obu_id",
            "vc_ex_lane_hex",
            "vc_en_lane_hex",
            "n_toll_support",
            "n_obu_sign",
            "n_en_vehicle_usertype",
            "n_ex_vehicle_usertype",
            "n_direction",
            "vc_vehicle_Sign_Id",
            "d_trans_Fee",
            "N_sign_Status",
            "vc_trade_id",
            "ts_timestamp",
            "n_vehicle_sign",
            "n_vehicle_class_gb",
            "vc_cert_no",
            "vc_special_type_gb",
            "n_gantry_lines",
            "n_detail_lines",
            "d_zj_fee",
            "n_obu_count_local",
            "n_obu_count_total",
            "n_obu_count_nocard",
            "n_obu_count_provs",
            "vc_obu_en_station",
            "n_obu_distance",
            "d_prov_pay_fee",
            "d_prov_fee",
            "d_cpu_fee_total",
            "vc_obu_ef04_rec",
            "vc_prov_ids",
            "vc_prov_fees",
            "vc_prov_od",
            "vc_short_fare_ver",
            "d_short_fee_total",
            "n_agency_gantry_num",
            "d_agency_gantry_fee",
            "n_display_amount_type",
            "n_trans_Pay_Type",
            "d_short_Fee_Mileage",
            "d_fee_Rate",
            "n_exit_Fee_Type",
            "d_total_pay_fee",
            "d_total_fee",
            "n_trade_mode",
            "vc_appoint",
            "d_prov_pass_fee",
            "n_ex_date"
        };
        for (String val : vals)
            System.out.println("\"data_" + val + " as " + CamelCaseUtils.toCamelCase(val) + "\",");
    }
}
