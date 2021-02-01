<?php

namespace app\commands;

use app\models\EnergyTrackingMeter;
use app\models\EnergyTrackingConsumptionActual;
use yii\console\Controller;
use Yii;

class MetersController extends Controller
{
    public function actionPopulateAllHourly()
    {
        if (empty($start_day)) {
            $start_day = date('YmdHi', strtotime('-20 hour'));
        }
        if (empty($end_day)) {
            $end_day = date('YmdHi', strtotime('-14 hour'));
        }
        $meters = EnergyTrackingMeter::find()
            ->select(['mprn_mpan', 'utility_type_id'])
            ->where(['consent_status' => 'SUCCESS'])
            ->groupBY(['mprn_mpan', 'utility_type_id'])
            ->all();
        foreach ($meters as $meter_key => $meter) {
            if ($meter->utility_type_id == 1) {
                $consumption = Yii::$app->n3rgy->getConsumptionWithStartEnd($meter->mprn_mpan, $start_day, $end_day, 'electricity');
                $tariff      = Yii::$app->n3rgy->getTariffsWithStartEnd($meter->mprn_mpan, $start_day, $end_day, 'electricity');
            } elseif ($meter->utility_type_id == 2) {
                $consumption = Yii::$app->n3rgy->getConsumptionWithStartEnd($meter->mprn_mpan, $start_day, $end_day, 'gas');
                $tariff      = Yii::$app->n3rgy->getTariffsWithStartEnd($meter->mprn_mpan, $start_day, $end_day, 'gas');
            }
            if (!empty($consumption->values)) {
                $batch_array = [];
                foreach ($consumption->values as $consumption_index => $consumption_data) {
                    $entry                            = $consumption_data;
                    $ts                               = $entry->timestamp;
                    $dateArr                          = explode(' ', $ts);
                    list($y, $m, $d)                  = explode('-', $dateArr[0]);
                    $time                             = $dateArr[1];
                    $consumption                      = [];
                    $consumption['hh_slot']           = Yii::$app->dates->timeToHourSlot($time);
                    $consumption['consumption_units'] = $entry->value;
                    $consumption['meter_id']          = null;
                    $consumption['household_id']      = null;
                    $consumption['reading_date']      = $dateArr[0];
                    $batch_array[] = $consumption;
                }
                $tracking_meters = EnergyTrackingMeter::find()
                    ->where(['consent_status' => 'SUCCESS', 'mprn_mpan' => $meter->mprn_mpan])
                    ->all();
                $master_batch = [];
                foreach ($tracking_meters as $tracking_meter_index => $tracking_meter) {
                    foreach ($batch_array as $batch_array_key => $batch_array_value) {
                        $batch_array[$batch_array_key]['meter_id']     = $tracking_meter->id;
                        $batch_array[$batch_array_key]['household_id'] = $tracking_meter->household_id;
                    }
                    $master_batch[] = $batch_array;
                }

                foreach ($master_batch as $master_batch_key => $master_batch_value) {
                    Yii::$app->db->createCommand('SET foreign_key_checks = 0;')->execute();
                    $command = Yii::$app->db->createCommand()->batchInsert(EnergyTrackingConsumptionActual::tableName(), ['hh_slot', 'consumption_units', 'meter_id', 'household_id', 'reading_date'], $master_batch_value);
                    $sql     = $command->getRawSql();
                    $sql .= ' ON DUPLICATE KEY UPDATE meter_id = meter_id';
                    $command->setRawSql($sql);
                    $consumption_insert_count = $command->execute();
                    Yii::$app->db->createCommand('SET foreign_key_checks = 1;')->execute();
                    /* tariff calculation */
                    $tariff_count = 0;
                    if ($consumption_insert_count > 0) {
                        foreach ($master_batch_value as $master_batch_value_key => $master_batch_value_value) {
                            foreach ($tariff->values as $tariff_key => $tariff_value) {
                                foreach ($tariff_value->prices as $tvp_key => $tvp_value) {
                                    $ts              = $tvp_value->timestamp;
                                    $val             = $tvp_value->value;
                                    $dateArr         = explode(' ', $ts);
                                    list($y, $m, $d) = explode('-', $dateArr[0]);
                                    $time            = $dateArr[1];
                                    $hh_slot         = Yii::$app->dates->timeToHourSlot($time);
                                    $existing_consumption               = EnergyTrackingConsumptionActual::find()->where([
                                        'reading_date' => $dateArr[0],
                                        'hh_slot'      => $hh_slot,
                                        'meter_id'     => $master_batch_value_value['meter_id']])->one();
                                    if ($existing_consumption) {
                                        $existing_consumption->consumption_pounds_total_cost = ($val * $a->consumption_units / 100);
                                        if (!$existing_consumption->save()) {
                                            log_error('unable to save consumption tariff');
                                        } else {
                                            $tariff_count++;
                                        }
                                    } else {
                                        log_error('consumption record not found');
                                    }
                                }
                            }
                        }
                    }
                    echo 'Batch insert consumption ' . $consumption_insert_count . ' tariff updates ' . $tariff_count;
                }
            } else {
                echo 'Empty Consumption ' . $meter->mprn_mpan;
            }
        }
    }
}
