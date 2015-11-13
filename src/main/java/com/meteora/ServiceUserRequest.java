package com.meteora;

import lombok.Data;

/**
 * @author sasaki kohei cyberagent.inc .
 */

@Data
public class ServiceUserRequest {
    private String findByUserId;
    private String findByUserPublicScoreGTE;
    private String findByUserPublicScoreLTE;
    private String findByUserFriendsNumberGTE;
    private String findByUserFriendsNumberLTE;
    private String findByUserFriendsIncludeUserIds;
    private String findByUserNotIncludeUserIds;
}
